import aio_pika
import json
from typing import List, Dict, Any
from parser_types import TimetableChangeData
from logger import trace
from profiler import profile
from loguru import logger
import asyncio


class Broker:
    def __init__(self, connection_string: str, queue_name: str):
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

                # Создаем очередь с durable=True для сохранения сообщений при перезагрузке
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
    async def send_changes(self, changes: List[TimetableChangeData]) -> bool:
        if not changes:
            return True

        try:
            await self.initialize()

            for change in changes:
                change_dict = self._change_to_dict(change)

                message = aio_pika.Message(
                    body=json.dumps(change_dict).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,  # Для сохранения сообщения при перезагрузке
                )

                await self.channel.default_exchange.publish(
                    message, routing_key=self.queue_name
                )

            return True
        except Exception as e:
            logger.error(f"Ошибка отправки изменений в RabbitMQ: {e}")
            return False

    @profile(func_name="broker.close")
    async def close(self):
        if self.connection:
            await self.connection.close()
            self.connection = None
            self.channel = None
            self.initialized = False

        logger.debug("RabbitMQ соединение закрыто")

    @profile(func_name="broker._change_to_dict")
    def _change_to_dict(self, change: TimetableChangeData) -> Dict[str, Any]:
        entity_dict = {
            "type": change.entity.type.value,
            "id": change.entity.id,
            "name": change.entity.name,
        }

        metadata_changes = []
        if change.metadata_changes:
            for field_change in change.metadata_changes:
                metadata_changes.append(
                    {
                        "field_name": field_change.field_name,
                        "old_value": str(field_change.old_value),
                        "new_value": str(field_change.new_value),
                    }
                )

        lesson_changes = []
        if change.lesson_changes:
            for lesson_change in change.lesson_changes:
                field_changes = []
                for field_change in lesson_change.field_changes:
                    field_changes.append(
                        {
                            "field_name": field_change.field_name,
                            "old_value": str(field_change.old_value),
                            "new_value": str(field_change.new_value),
                        }
                    )

                lesson_changes.append(
                    {
                        "change_type": lesson_change.change_type.value,
                        "field_changes": field_changes,
                        "old_lesson": str(lesson_change.old_lesson) if lesson_change.old_lesson else None,
                        "new_lesson": str(lesson_change.new_lesson) if lesson_change.new_lesson else None
                    }
                )

        return {
            "entity": entity_dict,
            "metadata_changes": metadata_changes,
            "lesson_changes": lesson_changes,
            "timestamp": str(asyncio.get_event_loop().time()),
        }
