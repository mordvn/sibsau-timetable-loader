from parser_types import TimetableData, EntityType, Lesson, Metadata, Entity
from typing import List, Dict
import hashlib
from logger import trace
from profiler import profile


class Auditorium:
    @staticmethod
    @profile(func_name="audithorium.from_timetables")
    async def from_timetables(timetables: List[TimetableData]) -> List[TimetableData]:
        auditoriums: Dict[str, List[Lesson]] = {}
        metadata_source = None

        for timetable in timetables:
            if not metadata_source and timetable.entity.type == EntityType.PROFESSOR:
                metadata_source = timetable

            for lesson in timetable.lessons:
                if not lesson.auditorium:
                    continue

                if lesson.auditorium not in auditoriums:
                    auditoriums[lesson.auditorium] = []

                auditoriums[lesson.auditorium].append(lesson)

        if not metadata_source and timetables:
            metadata_source = timetables[0]

        result = []
        for auditorium_name, lessons in auditoriums.items():
            if not lessons:
                continue

            auditorium_id = (
                int(hashlib.md5(auditorium_name.encode()).hexdigest(), 16) % 10000000
            )

            metadata = Metadata(
                years=metadata_source.metadata.years,
                date=metadata_source.metadata.date,
                week_number=metadata_source.metadata.week_number,
                semester=metadata_source.metadata.semester,
            )

            auditorium_data = TimetableData(
                entity=Entity(
                    type=EntityType.AUDITORIUM, id=auditorium_id, name=auditorium_name
                ),
                metadata=metadata,
                lessons=lessons,
            )

            result.append(auditorium_data)

        return result
