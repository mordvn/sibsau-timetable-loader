from parser_types import TimetableData, EntityType, Lesson, Metadata, Entity
from typing import List, Dict, Set, Tuple, Optional
import hashlib
from profiler import profile


class Auditorium:
    @staticmethod
    @profile(func_name="audithorium.from_timetables")
    async def from_timetables(timetables: List[TimetableData]) -> List[TimetableData]:
        auditoriums: Dict[str, List[Lesson]] = {}
        metadata_source = None

        lesson_map: Dict[Tuple, Lesson] = {}

        for timetable in timetables:
            if not metadata_source and timetable.entity.type == EntityType.PROFESSOR:
                metadata_source = timetable

            for lesson in timetable.lessons:
                if not lesson.auditorium:
                    continue

                lesson_key = (
                    lesson.schedule_type,
                    lesson.time_begin,
                    lesson.lesson_name,
                    lesson.week_number,
                    lesson.day_name,
                    lesson.day_date,
                    lesson.auditorium,
                    lesson.lesson_type,
                    lesson.subgroups,
                )

                if lesson.auditorium not in auditoriums:
                    auditoriums[lesson.auditorium] = []

                if lesson_key in lesson_map:
                    existing_lesson = lesson_map[lesson_key]
                    
                    if lesson.professors:
                        if not existing_lesson.professors:
                            existing_lesson.professors = []
                        for professor in lesson.professors:
                            if professor not in existing_lesson.professors:
                                existing_lesson.professors.append(professor)
                    
                    if lesson.groups:
                        if not existing_lesson.groups:
                            existing_lesson.groups = []
                        for group in lesson.groups:
                            if group not in existing_lesson.groups:
                                existing_lesson.groups.append(group)
                else:
                    auditorium_lesson = Lesson(
                        schedule_type=lesson.schedule_type,
                        time_begin=lesson.time_begin,
                        lesson_name=lesson.lesson_name,
                        schedule_form=lesson.schedule_form,
                        week_number=lesson.week_number,
                        day_name=lesson.day_name,
                        day_date=lesson.day_date,
                        duration=lesson.duration,
                        lesson_type=lesson.lesson_type,
                        groups=lesson.groups.copy() if lesson.groups else None,
                        professors=lesson.professors.copy() if lesson.professors else None,
                        auditorium=lesson.auditorium,
                        location=lesson.location,
                        subgroups=lesson.subgroups,
                    )
                    
                    lesson_map[lesson_key] = auditorium_lesson
                    auditoriums[lesson.auditorium].append(auditorium_lesson)

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
