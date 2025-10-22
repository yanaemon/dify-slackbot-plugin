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
                    message = message.split("> ", 1)[1] if "> " in message else message
                    channel = event.get("channel", "")
                    message_ts = event.get("ts", "")
                    token = settings.get("bot_token")
                    client = WebClient(token=token)

                    try:
                        # Try streaming mode first (for Agent/Chat apps)
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
                                query=message,
                                inputs={},
                                response_mode="streaming",
                            )

                            # Accumulate streaming chunks
                            full_answer = ""
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
                                    # Accumulate answer chunks (append, don't replace)
                                    if isinstance(chunk_data, dict):
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
                            else:
                                # If no answer was collected, show error message
                                result = client.chat_update(
                                    channel=channel,
                                    ts=response_ts,
                                    text="Sorry, I couldn't generate a response.",
                                    mrkdwn=True
                                )

                        except Exception as streaming_error:
                            # Streaming failed, try different app types
                            error_msg = str(streaming_error)
                            if "unexpected app type" in error_msg:
                                # Try as Workflow/Chatflow app
                                try:
                                    response = self.session.app.workflow.invoke(
                                        app_id=settings["app"]["app_id"],
                                        inputs={"query": message},
                                    )

                                    # Extract answer from workflow response
                                    answer = ""
                                    if isinstance(response, dict):
                                        # Try different possible response structures
                                        answer = response.get("answer", "") or response.get("text", "") or response.get("data", {}).get("outputs", {}).get("text", "")

                                    if not answer:
                                        answer = "Response received but could not extract answer."

                                    formatted_answer = converter.convert(answer)

                                    # Create proper mrkdwn block structure
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
                                except Exception as workflow_error:
                                    # If workflow also fails, re-raise the original streaming error
                                    raise streaming_error
                            else:
                                # Re-raise if it's a different error
                                raise

                        return Response(
                            status=200,
                            response=json.dumps(result),
                            content_type="application/json"
                        )
                    except Exception as e:
                        err = traceback.format_exc()

                        # Add x emoji to indicate error
                        try:
                            client.reactions_add(
                                channel=channel,
                                timestamp=message_ts,
                                name="x"
                            )
                        except SlackApiError:
                            pass

                        return Response(
                            status=200,
                            response="Sorry, I'm having trouble processing your request. Please try again later." + str(err),
                            content_type="text/plain",
                        )
                else:
                    return Response(status=200, response="ok")
            else:
                return Response(status=200, response="ok")
        else:
            return Response(status=200, response="ok")
