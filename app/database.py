from beanie import Document, init_beanie
from motor.motor_asyncio import AsyncIOMotorClient
from parser_types import (
    TimetableData,
    EntityType,
    Entity,
    Lesson,
    Metadata,
    WeekNumber,
    Semester,
    ScheduleType,
    ScheduleForm,
    DayName,
    LessonType,
    Subgroup,
)
from typing import List, Optional, Any
from pydantic import BaseModel
import pymongo
import pymongo.errors
import traceback
from loguru import logger
from datetime import time, timedelta, datetime
from logger import trace
from profiler import profile


class LessonModel(BaseModel):
    schedule_type: str
    time_begin: str
    lesson_name: str
    schedule_form: Optional[str] = None
    week_number: Optional[str] = None
    day_name: Optional[str] = None
    day_date: Optional[Any] = None
    duration: Optional[int] = None
    lesson_type: Optional[str] = None
    groups: Optional[List[str]] = None
    professors: Optional[List[str]] = None
    auditorium: Optional[str] = None
    location: Optional[str] = None
    subgroups: str = ""


class MetadataModel(BaseModel):
    years: str
    date: Any
    week_number: str
    semester: Optional[str] = None


class EntityModel(BaseModel):
    type: str
    id: int
    name: Optional[str] = None


class TimetableModel(Document):
    entity: EntityModel
    metadata: MetadataModel
    lessons: List[LessonModel]

    class Settings:
        name = "timetables"
        use_revision = False


class Database:
    def __init__(self, connection_string, db_name="sibsau-timetable"):
        self.connection_string = connection_string
        self.db_name = db_name
        self.client = None
        self.initialized = False

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    @profile
    async def initialize(self):
        if not self.initialized:
            try:
                self.client = AsyncIOMotorClient(self.connection_string)

                db = self.client[self.db_name]

                await init_beanie(
                    database=db,
                    document_models=[TimetableModel],
                    allow_index_dropping=True,
                )

                self.initialized = True
                logger.debug("MongoDB соединение инициализировано успешно")
            except Exception as e:
                logger.error(
                    f"Ошибка инициализации соединения MongoDB: {e}\n{traceback.format_exc()}"
                )
                if self.client:
                    self.client.close()
                self.client = None
                self.initialized = False
                raise e

    @trace
    @profile
    async def create_timetable(self, timetable: TimetableData) -> bool:
        await self.initialize()

        if (
            not timetable.entity
            or not timetable.entity.type
            or not timetable.entity.id
            or timetable.entity.id <= 0
        ):
            logger.error(
                f"Невозможно сохранить запись с невалидным Entity: {timetable.entity}"
            )
            return False

        if not timetable.metadata or not timetable.metadata.week_number:
            logger.error(
                f"Невозможно сохранить запись с невалидной Metadata: {timetable.metadata}"
            )
            return False

        try:
            if await self.is_exist(timetable.entity.type, timetable.entity.id):
                return await self.update_timetable(timetable)

            model = self._to_model(timetable)

            if (
                not model.entity
                or not model.entity.type
                or not model.entity.id
                or model.entity.id <= 0
            ):
                logger.error(
                    f"Некорректная модель после преобразования: {model.entity}"
                )
                return False

            await model.insert()
            return True
        except pymongo.errors.DuplicateKeyError as e:
            logger.warning(f"Дубликат ключа при создании: {e}")

            error_msg = str(e)
            duplicate_info = (
                error_msg.split("dup key: ")[-1] if "dup key: " in error_msg else ""
            )
            logger.debug(f"Конфликт по ключу: {duplicate_info}")

            try:
                return await self.update_timetable(timetable)
            except Exception as update_error:
                logger.error(
                    f"Ошибка при попытке обновления после конфликта: {update_error}"
                )
                return False
        except Exception as e:
            logger.error(f"Ошибка создания расписания: {e}")
            return False

    @profile
    async def is_exist(self, entity_type: EntityType, entity_id: int) -> bool:
        await self.initialize()
        count = await TimetableModel.find(
            {"entity.type": entity_type.value, "entity.id": entity_id}
        ).count()
        return count > 0

    @trace
    @profile
    async def update_timetable(self, timetable: TimetableData) -> bool:
        await self.initialize()

        if (
            not timetable.entity
            or not timetable.entity.type
            or not timetable.entity.id
            or timetable.entity.id <= 0
        ):
            logger.error(
                f"Невозможно обновить запись с невалидным Entity: {timetable.entity}"
            )
            return False

        try:
            existing = await TimetableModel.find_one(
                {
                    "entity.type": timetable.entity.type.value,
                    "entity.id": timetable.entity.id,
                }
            )

            if existing:
                await existing.delete()

            model = self._to_model(timetable)

            if (
                not model.entity
                or not model.entity.type
                or not model.entity.id
                or model.entity.id <= 0
            ):
                logger.error(
                    f"Некорректная модель после преобразования: {model.entity}"
                )
                return False

            await model.insert()
            return True
        except pymongo.errors.DuplicateKeyError as e:
            logger.error(f"Дубликат ключа при обновлении: {e}")
            return False
        except Exception as e:
            logger.error(f"Ошибка обновления расписания: {e}")
            return False

    @trace
    @profile
    async def get_timetable(
        self, entity_type: EntityType, entity_id: int
    ) -> Optional[TimetableData]:
        await self.initialize()
        model = await TimetableModel.find_one(
            {"entity.type": entity_type.value, "entity.id": entity_id}
        )
        if not model:
            return None
        return self._from_model(model)

    @trace
    @profile
    async def get_all(
        self, entity_type: Optional[EntityType] = None
    ) -> List[TimetableData]:
        await self.initialize()
        query = {}
        if entity_type:
            query = {"entity.type": entity_type.value}

        models = await TimetableModel.find(query).to_list()
        return [self._from_model(model) for model in models]

    @trace
    @profile
    async def delete_timetable(self, entity_type: EntityType, entity_id: int) -> bool:
        await self.initialize()
        try:
            result = await TimetableModel.find_one(
                {"entity.type": entity_type.value, "entity.id": entity_id}
            )
            if result:
                await result.delete()
                logger.debug(f"Расписание удалено: {entity_type.value} {entity_id}")
                return True
            return False
        except Exception as e:
            logger.error(f"Ошибка удаления расписания: {e}")
            return False

    @profile
    def _to_model(self, timetable: TimetableData) -> TimetableModel:
        if not timetable.entity:
            raise ValueError("Timetable Entity is None")
        if not timetable.entity.type:
            raise ValueError("Timetable Entity type is None")
        if not timetable.entity.id or timetable.entity.id <= 0:
            raise ValueError(f"Invalid Timetable Entity id: {timetable.entity.id}")

        entity_model = EntityModel(
            type=timetable.entity.type.value,
            id=timetable.entity.id,
            name=timetable.entity.name or "",
        )

        if entity_model.type is None or entity_model.id is None or entity_model.id <= 0:
            raise ValueError("Entity model validation error")

        if not timetable.metadata:
            raise ValueError("Timetable metadata is None")
        if not timetable.metadata.week_number:
            raise ValueError("Timetable metadata week_number is None")

        metadata_model = MetadataModel(
            years=timetable.metadata.years or "",
            date=timetable.metadata.date.date()
            if isinstance(timetable.metadata.date, datetime)
            else timetable.metadata.date,
            week_number=timetable.metadata.week_number.value,
            semester=timetable.metadata.semester.value
            if timetable.metadata.semester
            else None,
        )

        lesson_models = []
        for lesson in timetable.lessons:
            if not lesson.schedule_type:
                continue

            time_str = (
                lesson.time_begin.strftime("%H:%M") if lesson.time_begin else "00:00"
            )

            duration_seconds = (
                int(lesson.duration.total_seconds()) if lesson.duration else None
            )

            lesson_model = LessonModel(
                schedule_type=lesson.schedule_type.value,
                time_begin=time_str,
                lesson_name=lesson.lesson_name or "",
                schedule_form=lesson.schedule_form.value
                if lesson.schedule_form
                else None,
                week_number=lesson.week_number.value if lesson.week_number else None,
                day_name=lesson.day_name.value if lesson.day_name else None,
                day_date=lesson.day_date,
                duration=duration_seconds,
                lesson_type=lesson.lesson_type.value if lesson.lesson_type else None,
                groups=lesson.groups or [],
                professors=lesson.professors or [],
                auditorium=lesson.auditorium or "",
                location=lesson.location or "",
                subgroups=lesson.subgroups.value
                if lesson.subgroups
                else Subgroup.COMMON.value,
            )
            lesson_models.append(lesson_model)

        timetable_model = TimetableModel(
            entity=entity_model, metadata=metadata_model, lessons=lesson_models
        )

        if not hasattr(timetable_model, "entity") or timetable_model.entity is None:
            raise ValueError("Model validation error")

        return timetable_model

    @profile
    def _from_model(self, model: TimetableModel) -> TimetableData:
        entity_obj = Entity(
            type=EntityType(model.entity.type),
            id=model.entity.id,
            name=model.entity.name,
        )

        metadata_obj = Metadata(
            years=model.metadata.years,
            date=model.metadata.date.date()
            if hasattr(model.metadata.date, "date")
            else model.metadata.date,
            week_number=WeekNumber(model.metadata.week_number),
            semester=Semester(model.metadata.semester)
            if model.metadata.semester
            else None,
        )

        lessons = []
        for lesson_model in model.lessons:
            try:
                time_obj = (
                    datetime.strptime(lesson_model.time_begin, "%H:%M").time()
                    if lesson_model.time_begin
                    else time(0, 0)
                )
            except ValueError:
                time_obj = time(0, 0)

            duration_obj = (
                timedelta(seconds=lesson_model.duration)
                if lesson_model.duration is not None
                else None
            )

            lesson = Lesson(
                schedule_type=ScheduleType(lesson_model.schedule_type),
                time_begin=time_obj,
                lesson_name=lesson_model.lesson_name,
                schedule_form=ScheduleForm(lesson_model.schedule_form)
                if lesson_model.schedule_form
                else None,
                week_number=WeekNumber(lesson_model.week_number)
                if lesson_model.week_number
                else None,
                day_name=DayName(lesson_model.day_name)
                if lesson_model.day_name
                else None,
                day_date=lesson_model.day_date,
                duration=duration_obj,
                lesson_type=LessonType(lesson_model.lesson_type)
                if lesson_model.lesson_type
                else None,
                groups=lesson_model.groups,
                professors=lesson_model.professors,
                auditorium=lesson_model.auditorium,
                location=lesson_model.location,
                subgroups=Subgroup(lesson_model.subgroups)
                if lesson_model.subgroups
                else Subgroup.COMMON,
            )
            lessons.append(lesson)

        return TimetableData(entity=entity_obj, metadata=metadata_obj, lessons=lessons)

    @profile
    async def close(self):
        if self.client and self.initialized:
            self.client.close()
            self.client = None
            self.initialized = False
            logger.debug("MongoDB соединение закрыто")
