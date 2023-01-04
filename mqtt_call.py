import mqtt_as
import uasyncio
import _thread
import ujson
import uio


class Server:

    mqtt_client = None
    handler = None
    name = None
    debug = None

    def __init__(self, name, handler, wifi_ssid, wifi_password, mqtt_broker, debug=False):
        self.name = name
        self.handler = handler
        self.debug = debug
        mqtt_config = mqtt_as.config.copy()
        mqtt_config['ssid'] = wifi_ssid
        mqtt_config['wifi_pw'] = wifi_password
        mqtt_config['server'] = mqtt_broker
        mqtt_config['queue_len'] = 1

        mqtt_as.MQTTClient.DEBUG = debug
        self.mqtt_client = mqtt_as.MQTTClient(mqtt_config)

    def dump(self):
        print("Server", self.name, "exports:")
        for method_name in dir(self.handler):
            if not method_name.startswith('__'):
                print(' -', method_name)

    async def run(self):
        await self.mqtt_client.connect()

        async def subscribe_on_reconnect():
            while True:
                await self.mqtt_client.up.wait()
                self.mqtt_client.up.clear()

                topic = 'call/request/{}'.format(self.name)

                if self.debug:
                    print("Subscribing to:", topic)

                await self.mqtt_client.subscribe(topic, 1)

        uasyncio.create_task(subscribe_on_reconnect())

        async def handle_request(request):

            async def send(etc):

                topic = 'call/response/{}'.format(request['client']['id'])

                response = etc.copy()
                response["request"] = request['client']['request']

                msg = uio.BytesIO()
                ujson.dump(response, msg)
                msg = msg.getvalue()

                if self.debug:
                    print("Publishing:", topic, msg)

                await self.mqtt_client.publish(topic, msg)

            try:
                if self.debug:
                    print("Call request:", request)

                service = request["service"]
                params = request["params"]

                if self.debug:
                    print("Calling:", service, params, type(params))

                if not hasattr(self.handler, service):
                    raise ValueError("Unknown service '{}'".format(service))

                method = getattr(self.handler, service)
                result = method(**params)

                if hasattr(result, '__next__'):
                    result = await result

                if self.debug:
                    print("Result:", result)

                await send({"result": result})

            except Exception as e:
                await send({"error": {"message": str(e)}})

        async def read_messages():
            async for topic, msg, retained in self.mqtt_client.queue:
                if not retained:
                    try:
                        request = ujson.load(uio.BytesIO(msg.decode()))
                        uasyncio.create_task(handle_request(request))
                    except:
                        # discard invalid messages
                        pass

        await read_messages()

    def start(self):
        _thread.stack_size(32768)
        _thread.start_new_thread(lambda: uasyncio.run(self.run()), ())