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
                            if chunk.event == "agent_message" or chunk.event == "message":
                                if hasattr(chunk, 'answer'):
                                    full_answer = chunk.answer
                                elif hasattr(chunk, 'data') and isinstance(chunk.data, dict):
                                    full_answer = chunk.data.get("answer", full_answer)

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
                            result = {"ok": True}

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
