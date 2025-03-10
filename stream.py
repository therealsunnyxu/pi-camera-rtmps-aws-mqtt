from awscrt import mqtt
from awsiot import iotshadow, mqtt_connection_builder
from dotenv import dotenv_values
from models import ReadOnlyNamedShadow
from threading import Event
from uuid import uuid4
import json
import os
import time
import subprocess
from typing import List

# Set up working directory
CWD = os.path.dirname(os.path.realpath(__file__))
CONFIG = dotenv_values(os.path.join(CWD, ".env"))
rel_keys = ["CA", "CERT", "KEY"]
for key in rel_keys:
    CONFIG[key] = os.path.abspath(os.path.join(CWD, CONFIG[key]))

# Config
CAMERA_TOPIC = str(CONFIG["CAMERA_TOPIC"])
RELAY_NAME = str(CONFIG["RELAY_NAME"])
PRESENCE_CONNECTED = str(CONFIG["PRESENCE_CONNECTED"])
PRESENCE_DISCONNECTED = str(CONFIG["PRESENCE_DISCONNECTED"])

awake_event = Event()

def trigger_event(event: Event):
    event.set()

def clear_event(event: Event):
    event.clear()

def on_message_received(topic: str, payload: bytes, dup, qos, retain, **kwargs):
    """Catch-all callback for MQTT topic subscription

    Args:
        topic (str): The name of the MQTT topic
        payload (bytes): JSON payload encoded in bytes
    """
    if topic == PRESENCE_DISCONNECTED + RELAY_NAME:
        print("Relay disconnected.")
        clear_event(awake_event)
    elif topic == PRESENCE_CONNECTED + RELAY_NAME:
        print("Relay connected.")
        trigger_event(awake_event)
    elif topic == CAMERA_TOPIC and not awake_event.is_set():
        # A keep-alive command is coming from the relay
        # Only trigger it once
        payload: dict = json.loads(payload.decode('utf-8'))
        if payload.get("command") and "keepalive" in str(payload["command"]):
            trigger_event(awake_event)


def get_new_stream_key(shadow: ReadOnlyNamedShadow, client: iotshadow.IotShadowClient) -> str:
    """Returns a new stream key and adds it as a value in a named shadow

    Args:
        shadow (ReadOnlyNamedShadow): The named shadow for containing the key
        client (iotshadow.IotShadowClient): The client that connects to the shadow

    Returns:
        str: The stream key
    """

    token = str(uuid4())
    with shadow.locked_data.lock:
        shadow.locked_data.request_tokens.add(token)
    publish_get_future = client.publish_get_named_shadow(
        request=iotshadow.GetNamedShadowRequest(
            shadow_name=shadow.shadow,
            thing_name=RELAY_NAME,
            client_token=token,
        ),
        qos=mqtt.QoS.AT_LEAST_ONCE,
    )

    time.sleep(2)
    publish_get_future.result()
    time.sleep(2)
    new_stream_key = None
    with shadow.locked_data.lock:
        new_stream_key = shadow.locked_data.shadow_value
    return new_stream_key
    
    
if __name__ == '__main__':
    RELAY_KEY_SHADOW=str(CONFIG["RELAY_KEY_SHADOW"])
    CLIENT_ID=str(CONFIG["CLIENT_ID"])
    CAMERA_TOPIC = str(CONFIG["CAMERA_TOPIC"])

    # Make the connection to AWS IoT Core's MQTT broker
    try:
        mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint=CONFIG["ENDPOINT"],
            cert_filepath=CONFIG["CERT"],
            pri_key_filepath=CONFIG["KEY"],
            ca_filepath=CONFIG["CA"],
            client_id=CLIENT_ID)

        connect_future = mqtt_connection.connect()
        shadow_client = iotshadow.IotShadowClient(mqtt_connection)
        stream_key_shadow = ReadOnlyNamedShadow(shadow_client, RELAY_KEY_SHADOW, CLIENT_ID)

        connect_future.result()
        print("Connected!")
    except Exception as e:
        exit(e)

    # Subscribe to necessary topics
    try:
        # Subscribe to camera topic, for keep-alive signal
        print("Subscribing to topic '{}'...".format(CAMERA_TOPIC))
        subscribe_future, packet_id = mqtt_connection.subscribe(
            topic=CAMERA_TOPIC, qos=mqtt.QoS.AT_LEAST_ONCE, callback=on_message_received
        )

        subscribe_result = subscribe_future.result()
        print("Subscribed with {}".format(str(subscribe_result["qos"])))

        # Subscribe to relay connect and disconnect topics
        relay_connected = PRESENCE_CONNECTED + RELAY_NAME
        print("Subscribing to topic '{}'...".format(relay_connected))
        subscribe_future, packet_id = mqtt_connection.subscribe(
            topic=relay_connected, qos=mqtt.QoS.AT_LEAST_ONCE, callback=on_message_received
        )

        subscribe_result = subscribe_future.result()
        print("Subscribed with {}".format(str(subscribe_result)))

        relay_disconnected = PRESENCE_DISCONNECTED + RELAY_NAME
        print("Subscribing to topic '{}'...".format(relay_disconnected))
        subscribe_future, packet_id = mqtt_connection.subscribe(
            topic=relay_disconnected, qos=mqtt.QoS.AT_LEAST_ONCE, callback=on_message_received
        )

        subscribe_result = subscribe_future.result()
        print("Subscribed with {}".format(str(subscribe_result)))

        # Subscribe to relay device shadow, for refreshing stream key
        print("Subscribing to get responses...")
        get_accepted_subscribed_future, _ = (
            shadow_client.subscribe_to_get_named_shadow_accepted(
                request=iotshadow.GetNamedShadowSubscriptionRequest(
                    shadow_name=stream_key_shadow.shadow, thing_name=RELAY_NAME
                ),
                qos=mqtt.QoS.AT_LEAST_ONCE,
                callback=stream_key_shadow.on_get_shadow_accepted,
            )
        )

        get_rejected_subscribed_future, _ = (
            shadow_client.subscribe_to_get_named_shadow_rejected(
                request=iotshadow.GetNamedShadowSubscriptionRequest(
                    shadow_name=stream_key_shadow.shadow, thing_name=RELAY_NAME
                ),
                qos=mqtt.QoS.AT_LEAST_ONCE,
                callback=stream_key_shadow.on_get_shadow_rejected,
            )
        )

        res = get_accepted_subscribed_future.result()
        res = get_rejected_subscribed_future.result()
    except Exception as e:
        exit(e)

    # Start the stream and keep it alive
    while True:
        curr_stream_key: str = get_new_stream_key(stream_key_shadow, shadow_client)
        stream_uri = str(CONFIG["STREAM"]) + curr_stream_key
        libcamera_vid: subprocess.Popen = None
        try:
            # Start libcamera-vid process
            libcamera_vid = subprocess.Popen(
                [
                    "libcamera-vid", "-v", "0", "-t", "0", "--width", "640", "--height", "360",
                    "--framerate", "12", "--codec", "h264", "-o", "-"
                ],
                stdout=subprocess.PIPE
            )

            # Start ffmpeg process
            ffmpeg = subprocess.run(
                [
                    "ffmpeg", "-i", "-", "-f", "alsa", "-ac", "1", "-ar", "44100", "-sample_fmt", "s16", "-i", "sysdefault",
                    "-c:v", "copy", "-g", "24", "-c:a", "aac", "-b:a", "64k", "-ar", "44100", "-ac", "1",
                    "-b:v", "800k", "-preset", "ultrafast", "-tune", "zerolatency",
                    "-f", "flv", stream_uri,
                    "-rtmp_buffer", "300", "-rtmp_live", "live"
                ],
                stdin=libcamera_vid.stdout,
                check=True
            )
        except KeyboardInterrupt as e:
            # Manually stop stream
            clear_event(awake_event)
            if type(libcamera_vid) == subprocess.Popen:
                libcamera_vid.kill()
            print("Manually stopping stream.")
            quit()
        except Exception as e:
            # Stream errored, which means the camera lost connection with the relay or the relay was stopped
            # Wait for relay to send signal to reconnect
            clear_event(awake_event)
            if type(libcamera_vid) == subprocess.Popen:
                libcamera_vid.kill()
                
            print("Failed to start stream. Waiting for relay to send connect or keep-alive message.")
            awake_event.wait()
            curr_stream_key = get_new_stream_key(stream_key_shadow, shadow_client)

        # Give the script some time to get the ducks in line
        time.sleep(int(CONFIG["RETRY_SECONDS"]))
