import time
import traceback
import logging
import ssl
import json
import pika

logger = logging.getLogger()


class RabbitMQ:
    def __init__(self, host, port, vhost, username, password, usessl=True,
                 heartbeat=60):
        self.username = username
        self.password = password
        self.host = host
        self.vhost = vhost
        self.port = port
        self.usessl = usessl
        self.heartbeat = int(heartbeat)
        self.connection = None
        self.channel = None

    def _connect(self):
        retrycount = 0
        credentials = pika.PlainCredentials(username=self.username,
                                            password=self.password)
        if self.usessl:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            parameters = pika.ConnectionParameters(host=self.host,
                                                   port=self.port,
                                                   virtual_host=self.vhost,
                                                   credentials=credentials,
                                                   ssl_options=pika.SSLOptions(ssl_context),
                                                   heartbeat=self.heartbeat,
                                                   blocked_connection_timeout=60)
        else:
            parameters = pika.ConnectionParameters(host=self.host,
                                                   port=self.port,
                                                   virtual_host=self.vhost,
                                                   credentials=credentials,
                                                   heartbeat=self.heartbeat,
                                                   blocked_connection_timeout=60)
        
        for i in range(1, 4):
            try:
                self.connection = pika.BlockingConnection(parameters)
                logger.debug("Connected to RabbitMQ")
                return True
            except Exception:
                logger.debug(traceback.print_exc())
                logging.error("Error while connecting to RabbitMQ")
                time.sleep(10)
            retrycount += 1
        logger.error("Connection to RabbitMQ failed")
        return None

    def _publish(self):
        pass

    def create_queue(self, queue):
        for i in range(1, 4):
            try:
                if not self.connection or not self.channel:
                    if self._connect():
                        self.channel = self.connection.channel()
                self.channel.queue_declare(queue=queue)
                return True
            except pika.exceptions.ConnectionClosed:
                if self._connect():
                    self.channel = self.connection.channel()
                else:
                    logger.error("can not establish a new connection after ConnectionClosed error!")
            except pika.exceptions.ChannelClosed:
                logger.warning("Error while openning a new RabbitMQ channel")
                logger.debug("Connection_sate {}".format(self.connection))
                if self._connect():
                    self.channel = self.connection.channel()
                else:
                    logger.error("can not establish a new connection after ChannelClosed error!")
            except pika.exceptions.ChannelWrongStateError:
                logger.warning(
                    "Error while opening a new RabbitMQ channel. ChannelWrongStateError")
                logger.debug("Connection_sate {}".format(self.connection))
                if self._connect():
                    self.channel = self.connection.channel()
                    logger.debug('new RabbitMQ connection established')
                else:
                    logger.error("can not establish a new connection after ChannelWrongStateError error!")
            except:
                logger.error(
                    "Unexpected error while writing data to RabbitMQ")
                logger.debug(traceback.print_exc())
                if self._connect():
                    self.channel = self.connection.channel()
                    logger.debug('new RabbitMQ connection established')
                else:
                    logger.error("can not establish a new connection after Unknown error!")
        return False

    def senddata(self, data, exchange="", routingkey="", queue=""):
        for i in range(1, 4):
            try:
                if not self.connection or not self.channel:
                    if self._connect():
                        self.channel = self.connection.channel()
                t = type(data)
                if t is dict or t is list:
                    body = json.dumps(data)
                else:
                    body = data
                self.channel.basic_publish(
                    exchange=exchange, routing_key=routingkey,
                    body=body)
                return True
            except pika.exceptions.ConnectionClosed:
                if self._connect():
                    self.channel = self.connection.channel()
                else:
                    logger.error("can not establish a new connection after ConnectionClosed error!")
            except pika.exceptions.ChannelClosed:
                logger.warning("Error while openning a new RabbitMQ channel")
                logger.debug("Connection_sate {}".format(self.connection))
                if self._connect():
                    self.channel = self.connection.channel()
                else:
                    logger.error("can not establish a new connection after ChannelClosed error!")
            except pika.exceptions.ChannelWrongStateError:
                logger.warning(
                    "Error while opening a new RabbitMQ channel. ChannelWrongStateError")
                logger.debug("Connection_sate {}".format(self.connection))
                if self._connect():
                    self.channel = self.connection.channel()
                    logger.debug('new RabbitMQ connection established')
                else:
                    logger.error("can not establish a new connection after ChannelWrongStateError error!")
            except:
                logger.error(
                    "Unexpected error while writing data to RabbitMQ")
                logger.debug(traceback.print_exc())
                if self._connect():
                    self.channel = self.connection.channel()
                    logger.debug('new RabbitMQ connection established')
                else:
                    logger.error("can not establish a new connection after Unknown error!")
        return False


    def listen(self, queue, callback, prefetch_count=1):
        if not self.connection:
            if not self._connect():
                return False
        while True:
            try:
                channel = self.connection.channel()
                channel.basic_qos(prefetch_count=prefetch_count)
                channel.basic_consume(on_message_callback=callback,
                                      queue=queue)
                logger.debug("Start listening to MQ '{}'".format(queue))
                channel.start_consuming()
            except pika.exceptions.ConnectionClosed:
                self._connect()
            except pika.exceptions.ChannelClosed:
                logger.error("Error while openning a new RabbitMQ channel")
                logger.debug("Connection_sate {}".format(self.connection))
                self._connect()
            except pika.exceptions.ChannelWrongStateError:
                logger.error("Error while opening a new RabbitMQ channel. ChannelWrongStateError")
                logger.debug("Connection_sate {}".format(self.connection))
                self._connect()
            except Exception as err:
                logger.error("Error listening to queue '{}': {}".format(queue, err))
                logger.debug(traceback.print_exc())
                try:
                    self.connection.close()
                except:
                    pass
                self._connect()
