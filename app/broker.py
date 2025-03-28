import aio_pika
import json
from typing import List
from parser_types import TimetableChangeData
from profiler import profile
from loguru import logger
from datetime import date, time, datetime, timedelta
from enum import Enum
from logger import trace


class DataEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Enum):
            return {"__enum__": obj.value}
        if isinstance(obj, (date, datetime)):
            return {"__datetime__": obj.isoformat()}
        if isinstance(obj, time):
            return {"__time__": obj.isoformat()}
        if isinstance(obj, timedelta):
            return {"__timedelta__": obj.total_seconds()}
        if hasattr(obj, "__dict__"):
            class_name = obj.__class__.__name__
            return {f"__{class_name.lower()}__": obj.__dict__}
        return super().default(obj)


class Broker:
    def __init__(self, connection_string: str, queue_name: str = "timetable_changes"):
        self.connection_string = connection_string
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.initialized = False

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    @profile(func_name="broker.initialize")
    async def initialize(self):
        if not self.initialized:
            try:
                self.connection = await aio_pika.connect_robust(self.connection_string)
                self.channel = await self.connection.channel()
                await self.channel.declare_queue(self.queue_name, durable=True)
                self.initialized = True
                logger.debug("Подключение к RabbitMQ инициализировано успешно")
            except Exception as e:
                logger.error(f"Ошибка инициализации подключения к RabbitMQ: {e}")
                if self.connection:
                    await self.connection.close()
                self.connection = None
                self.channel = None
                self.initialized = False
                raise e

    @profile(func_name="broker.send_changes")
    @trace
    async def send_changes(self, changes: List[TimetableChangeData]) -> bool:
        if not changes:
            return True

        try:
            await self.initialize()

            for change in changes:
                try:
                    json_data = json.dumps(change, cls=DataEncoder)

                    message = aio_pika.Message(
                        body=json_data.encode(),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    )
                    await self.channel.default_exchange.publish(
                        message, routing_key=self.queue_name
                    )
                except Exception as e:
                    import traceback

                    logger.error(f"Error serializing/sending change: {e}")
                    logger.error(traceback.format_exc())
                    raise

            return True
        except Exception as e:
            import traceback

            logger.error(f"Ошибка отправки изменений в RabbitMQ: {e}")
            logger.error(traceback.format_exc())
            return False

    @profile(func_name="broker.close")
    async def close(self):
        if self.connection:
            await self.connection.close()
            self.connection = None
            self.channel = None
            self.initialized = False
        logger.debug("RabbitMQ соединение закрыто")

    @staticmethod
    def object_hook(obj):
        from parser_types import (
            Lesson,
            ScheduleType,
            ScheduleForm,
            WeekNumber,
            DayName,
            LessonType,
            Subgroup,
            Entity,
            EntityType,
            FieldChange,
            ChangeType,
            LessonChange,
            TimetableChangeData,
        )

        if "__enum__" in obj:
            for enum_type in [
                ScheduleType,
                ScheduleForm,
                WeekNumber,
                DayName,
                LessonType,
                Subgroup,
                EntityType,
                ChangeType,
            ]:
                try:
                    return enum_type(obj["__enum__"])
                except (ValueError, TypeError):
                    continue
            return obj["__enum__"]
        elif "__datetime__" in obj:
            return datetime.fromisoformat(obj["__datetime__"])
        elif "__time__" in obj:
            return time.fromisoformat(obj["__time__"])
        elif "__timedelta__" in obj:
            return timedelta(seconds=obj["__timedelta__"])

        # Now handle dataclass objects
        class_types = {
            "__lesson__": Lesson,
            "__entity__": Entity,
            "__fieldchange__": FieldChange,
            "__lessonchange__": LessonChange,
            "__timetablechangedata__": TimetableChangeData,
        }

        for type_key, class_type in class_types.items():
            if type_key in obj:
                data_dict = obj[type_key]
                # Process any nested types
                for key, value in data_dict.items():
                    if isinstance(value, dict):
                        data_dict[key] = Broker.object_hook(value)
                    elif (
                        isinstance(value, list) and value and isinstance(value[0], dict)
                    ):
                        data_dict[key] = [Broker.object_hook(item) for item in value]

                # Create and return the instance
                return class_type(**data_dict)

        return obj

    @staticmethod
    def loads(data_str):
        return json.loads(data_str, object_hook=Broker.object_hook)

    @staticmethod
    @profile(func_name="broker.process_message")
    def process_message(message_body):
        try:
            if isinstance(message_body, bytes):
                message_body = message_body.decode("utf-8")

            change_data = Broker.loads(message_body)

            return change_data
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return None
