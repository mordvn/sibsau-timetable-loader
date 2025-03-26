import asyncio
from logger import trace
from profiler import profile
from loguru import logger
from database import Database
from broker import Broker
from config import settings

from parser_types import Entity, EntityType, TimetableData
from parser import Parser
from audithorium import Auditorium
from comparer import Comparer
from typing import List
import time


class Runner:
    @staticmethod
    @trace
    @profile
    async def process_all_entities():
        logger.info("Starting process_all_entities")
        start_time = time.time()

        async with (
            Database(settings.MONGODB_URI) as db,
            Broker(settings.RABBITMQ_URI, "timetable_changes") as broker,
        ):
            process_entities = Runner._get_process_entities()
            timetables = await Runner._fetch_timetables(process_entities)
            auditorium_timetables = await Auditorium.from_timetables(timetables)
            timetables.extend(auditorium_timetables)

            db_timetables = await db.get_all()
            changes = await Runner._detect_changes(db_timetables, timetables)

            if changes:
                await broker.send_changes(changes)

            await Runner._add_new_timetables(db, timetables)

        logger.info(
            "Finished process_all_entities after %s seconds", time.time() - start_time
        )

    @staticmethod
    @profile
    def _get_process_entities() -> List[Entity]:
        entities = []
        for group_id in range(settings.START_GROUP_ID, settings.END_GROUP_ID):
            entities.append(Entity(EntityType.GROUP, group_id))

        for professor_id in range(
            settings.START_PROFESSOR_ID, settings.END_PROFESSOR_ID
        ):
            entities.append(Entity(EntityType.PROFESSOR, professor_id))

        return entities

    @staticmethod
    @profile
    async def _fetch_timetables(entities: List[Entity]) -> List[TimetableData]:
        timetables = []
        for entity in entities:
            timetable = await Parser.get_timetable(entity)
            timetables.append(timetable)
            await asyncio.sleep(settings.ANTI_DDOS_FETCH_INTERVAL)
        return timetables

    @staticmethod
    @profile
    async def _detect_changes(
        db_timetables: List[TimetableData], timetables: List[TimetableData]
    ) -> List:
        changes = []
        for db_timetable in db_timetables:
            for timetable in timetables:
                if (
                    db_timetable.entity.id == timetable.entity.id
                    and db_timetable.entity.type == timetable.entity.type
                ):
                    changes_data = await Comparer.compare_timetables(
                        db_timetable, timetable
                    )
                    if changes_data:
                        changes.append(changes_data)
        return changes

    @staticmethod
    @profile
    async def _add_new_timetables(db: Database, timetables: List[TimetableData]):
        for timetable in timetables:
            if not await db.is_exist(timetable.entity.type, timetable.entity.id):
                await db.create_timetable(timetable)
            else:
                await db.update_timetable(timetable)
