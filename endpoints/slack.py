import json
import time
import traceback
from typing import Mapping
from werkzeug import Request, Response
from dify_plugin import Endpoint
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from markdown_to_mrkdwn import SlackMarkdownConverter

converter = SlackMarkdownConverter()


class SlackEndpoint(Endpoint):
    def _get_thread_context(self, client, channel, thread_ts, current_message_ts, bot_user_id):
        """
        Get conversation context from the thread, including all messages from the last bot post.

        Returns: Formatted context string with conversation history
        """
        try:
            # Get thread replies
            result = client.conversations_replies(
                channel=channel,
                ts=thread_ts,
                inclusive=True
            )

            messages = result.get("messages", [])
            if not messages:
                return ""

            # Find the last bot message before the current mention
            last_bot_index = -1
            for i in range(len(messages) - 1, -1, -1):
                msg = messages[i]
                # Skip the current message
                if msg.get("ts") == current_message_ts:
                    continue
                # Check if this is a bot message
                if msg.get("bot_id") or msg.get("user") == bot_user_id:
                    last_bot_index = i
                    break

            # If no bot message found, start from the beginning
            # Start from the message after the last bot post to exclude bot's own response
            start_index = last_bot_index + 1 if last_bot_index >= 0 else 0

            # Build context from messages after last bot post up to (not including) current message
            context_parts = []
            for msg in messages[start_index:]:
                # Skip the current message (it will be added separately)
                if msg.get("ts") == current_message_ts:
                    continue

                text = msg.get("text", "")
                user = msg.get("user", "unknown")

                # Remove bot mentions from text
                if text.startswith("<@"):
                    text = text.split("> ", 1)[1] if "> " in text else text

                context_parts.append(f"[{user}]: {text}")

            if context_parts:
                return "\n".join(context_parts) + "\n\n"
            return ""

        except Exception as e:
            print(f"Error getting thread context: {e}")
            print(traceback.format_exc())
            return ""

    def _invoke(self, r: Request, values: Mapping, settings: Mapping) -> Response:
        """
        Invokes the endpoint with the given request.
        """
        retry_num = r.headers.get("X-Slack-Retry-Num")
        if (not settings.get("allow_retry") and (r.headers.get("X-Slack-Retry-Reason") == "http_timeout" or ((retry_num is not None and int(retry_num) > 0)))):
            return Response(status=200, response="ok")
        data = r.get_json()

        # Handle Slack URL verification challenge
        if data.get("type") == "url_verification":
            return Response(
                response=json.dumps({"challenge": data.get("challenge")}),
                status=200,
                content_type="application/json"
            )
        
        if (data.get("type") == "event_callback"):
            event = data.get("event")
            if (event.get("type") == "app_mention"):
                message = event.get("text", "")
                if message.startswith("<@"):
                    try:
                        message = message.split("> ", 1)[1] if "> " in message else message
                        channel = event.get("channel", "")
                        message_ts = event.get("ts", "")
                        thread_ts = event.get("thread_ts", message_ts)  # Use thread_ts if in thread, otherwise use message_ts
                        token = settings.get("bot_token")
                        client = WebClient(token=token)

                        # Get bot user ID for context retrieval
                        auth_response = client.auth_test()
                        bot_user_id = auth_response.get("user_id", "")

                        # Get thread context (messages from last bot post to current)
                        thread_context = self._get_thread_context(
                            client, channel, thread_ts, message_ts, bot_user_id
                        )

                        # Combine context with current message
                        full_query = thread_context + f"[Current message]: {message}"

                        # Get or create conversation ID for this thread
                        storage_key = f"slack_thread_{thread_ts}"
                        try:
                            stored_value = self.session.storage.get(storage_key)
                            conversation_id = stored_value.decode('utf-8') if stored_value else ""
                        except Exception as storage_error:
                            # If storage fails, continue without conversation persistence
                            conversation_id = ""
                            print(f"Storage get failed: {storage_error}")

                    except Exception as init_error:
                        print(f"Initialization error: {init_error}")
                        print(traceback.format_exc())
                        return Response(
                            status=500,
                            response=f"Initialization error: {str(init_error)}",
                            content_type="text/plain",
                        )

                    try:
                        # Try streaming mode first (for Agent/Chat apps)
                        result = None
                        try:
                            # Post initial placeholder message
                            initial_msg = client.chat_postMessage(
                                channel=channel,
                                thread_ts=message_ts,
                                text="Thinking...",
                                mrkdwn=True
                            )
                            response_ts = initial_msg["ts"]

                            # Start streaming response
                            response_stream = self.session.app.chat.invoke(
                                app_id=settings["app"]["app_id"],
                                query=full_query,
                                inputs={},
                                response_mode="streaming",
                                conversation_id=conversation_id,
                            )

                            # Accumulate streaming chunks
                            full_answer = ""
                            response_conversation_id = ""
                            last_update_time = time.time()
                            update_interval = 1.0  # Update every 1 second

                            for chunk in response_stream:
                                # Handle different chunk structures
                                chunk_data = None
                                if hasattr(chunk, 'data'):
                                    chunk_data = chunk.data
                                elif isinstance(chunk, dict):
                                    chunk_data = chunk

                                if chunk_data:
                                    # Capture conversation_id from response
                                    if isinstance(chunk_data, dict):
                                        if 'conversation_id' in chunk_data and chunk_data['conversation_id']:
                                            response_conversation_id = chunk_data['conversation_id']

                                        # Accumulate answer chunks (append, don't replace)
                                        if 'answer' in chunk_data and chunk_data['answer']:
                                            full_answer += chunk_data['answer']
                                        elif 'text' in chunk_data and chunk_data['text']:
                                            full_answer += chunk_data['text']
                                    elif isinstance(chunk_data, str):
                                        full_answer += chunk_data

                                # Update message periodically
                                current_time = time.time()
                                if current_time - last_update_time >= update_interval and full_answer:
                                    formatted_answer = converter.convert(full_answer)
                                    blocks = [{
                                        "type": "section",
                                        "text": {
                                            "type": "mrkdwn",
                                            "text": formatted_answer
                                        }
                                    }]

                                    try:
                                        client.chat_update(
                                            channel=channel,
                                            ts=response_ts,
                                            text=formatted_answer,
                                            blocks=blocks,
                                            mrkdwn=True
                                        )
                                        last_update_time = current_time
                                    except SlackApiError:
                                        pass  # Continue if update fails

                            # Final update with complete answer
                            if full_answer:
                                formatted_answer = converter.convert(full_answer)
                                blocks = [{
                                    "type": "section",
                                    "text": {
                                        "type": "mrkdwn",
                                        "text": formatted_answer
                                    }
                                }]

                                result = client.chat_update(
                                    channel=channel,
                                    ts=response_ts,
                                    text=formatted_answer,
                                    blocks=blocks,
                                    mrkdwn=True
                                )

                                # Store conversation_id for this thread
                                if response_conversation_id:
                                    try:
                                        self.session.storage.set(storage_key, response_conversation_id.encode('utf-8'))
                                    except Exception as storage_error:
                                        print(f"Storage set failed: {storage_error}")
                            else:
                                # If no answer was collected, show error message
                                result = client.chat_update(
                                    channel=channel,
                                    ts=response_ts,
                                    text="Sorry, I couldn't generate a response.",
                                    mrkdwn=True
                                )

                        except Exception as streaming_error:
                            # Streaming failed, try Chatflow with completion API as fallback
                            try:
                                # Use completion API for Chatflow apps
                                response = self.session.app.completion.invoke(
                                    app_id=settings["app"]["app_id"],
                                    query=full_query,
                                    inputs={},
                                    response_mode="blocking",
                                    conversation_id=conversation_id,
                                )

                                answer = response.get("answer", "")
                                formatted_answer = converter.convert(answer)

                                blocks = [{
                                    "type": "section",
                                    "text": {
                                        "type": "mrkdwn",
                                        "text": formatted_answer
                                    }
                                }]

                                result = client.chat_postMessage(
                                    channel=channel,
                                    thread_ts=message_ts,
                                    text=formatted_answer,
                                    blocks=blocks,
                                    mrkdwn=True
                                )

                                # Store conversation_id for this thread
                                response_conversation_id = response.get("conversation_id", "")
                                if response_conversation_id:
                                    try:
                                        self.session.storage.set(storage_key, response_conversation_id.encode('utf-8'))
                                    except Exception as storage_error:
                                        print(f"Storage set failed: {storage_error}")
                            except Exception as completion_error:
                                # If completion also fails, re-raise original streaming error
                                raise streaming_error

                        return Response(
                            status=200,
                            response=json.dumps({"ok": True}),
                            content_type="application/json"
                        )
                    except Exception as e:
                        err = traceback.format_exc()
                        print(f"Main error handler: {e}")
                        print(err)

                        # Try to post error message to Slack
                        try:
                            client.chat_postMessage(
                                channel=channel,
                                thread_ts=message_ts,
                                text=f"Sorry, I'm having trouble processing your request. Error: {str(e)}",
                                mrkdwn=False
                            )
                        except Exception:
                            pass

                        return Response(
                            status=200,
                            response=json.dumps({"ok": False, "error": str(e)}),
                            content_type="application/json",
                        )
                else:
                    return Response(status=200, response="ok")
            else:
                return Response(status=200, response="ok")
        else:
            return Response(status=200, response="ok")
