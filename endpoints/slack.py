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

                    # Add eyes emoji to indicate processing has started
                    try:
                        client.reactions_add(
                            channel=channel,
                            timestamp=message_ts,
                            name="eyes"
                        )
                    except SlackApiError:
                        pass  # Continue even if reaction fails

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
                        print(f"Starting streaming invoke for app_id: {settings['app']['app_id']}")
                        response_stream = self.session.app.chat.invoke(
                            app_id=settings["app"]["app_id"],
                            query=message,
                            inputs={},
                            response_mode="streaming",
                        )
                        print(f"Response stream type: {type(response_stream)}")

                        # Accumulate streaming chunks
                        full_answer = ""
                        last_update_time = time.time()
                        update_interval = 1.0  # Update every 1 second

                        try:
                            for chunk in response_stream:
                                print(f"Received chunk: {chunk}")
                                # Handle different chunk structures
                                chunk_data = None
                                if hasattr(chunk, 'data'):
                                    chunk_data = chunk.data
                                elif isinstance(chunk, dict):
                                    chunk_data = chunk

                                print(f"Chunk data: {chunk_data}")

                                if chunk_data:
                                    # Try to extract answer from various possible structures
                                    if isinstance(chunk_data, dict):
                                        if 'answer' in chunk_data:
                                            full_answer = chunk_data['answer']
                                            print(f"Updated full_answer from 'answer': {len(full_answer)} chars")
                                        elif 'text' in chunk_data:
                                            full_answer = chunk_data['text']
                                            print(f"Updated full_answer from 'text': {len(full_answer)} chars")
                                    elif isinstance(chunk_data, str):
                                        full_answer += chunk_data
                                        print(f"Appended to full_answer: {len(full_answer)} chars total")

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
                        except Exception as stream_error:
                            # Log streaming error but continue to show what we have
                            print(f"Streaming error: {stream_error}")
                            print(traceback.format_exc())

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

                        # Remove eyes emoji after successful response
                        try:
                            client.reactions_remove(
                                channel=channel,
                                timestamp=message_ts,
                                name="eyes"
                            )
                        except SlackApiError:
                            pass  # Continue even if reaction removal fails

                        return Response(
                            status=200,
                            response=json.dumps(result),
                            content_type="application/json"
                        )
                    except Exception as e:
                        err = traceback.format_exc()

                        # Remove eyes emoji and add x emoji to indicate error
                        try:
                            client.reactions_remove(
                                channel=channel,
                                timestamp=message_ts,
                                name="eyes"
                            )
                        except SlackApiError:
                            pass

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
