from parser_types import (
    TimetableData,
    TimetableChangeData,
    ChangeType,
    FieldChange,
    LessonChange,
)
from typing import Optional
from logger import trace
from profiler import profile


class Comparer:
    @staticmethod
    @profile(func_name="comparer.compare_timetables")
    async def compare_timetables(
        timetable1: TimetableData, timetable2: TimetableData
    ) -> Optional[TimetableChangeData]:
        id_info = timetable2.entity
        metadata_changes = []
        lesson_changes = []

        metadata1 = timetable1.metadata
        metadata2 = timetable2.metadata

        if metadata1.years != metadata2.years:
            metadata_changes.append(
                FieldChange(
                    field_name="years",
                    old_value=metadata1.years,
                    new_value=metadata2.years,
                )
            )

        date1 = (
            metadata1.date.date() if hasattr(metadata1.date, "date") else metadata1.date
        )
        date2 = (
            metadata2.date.date() if hasattr(metadata2.date, "date") else metadata2.date
        )
        if date1 != date2:
            metadata_changes.append(
                FieldChange(
                    field_name="date",
                    old_value=metadata1.date,
                    new_value=metadata2.date,
                )
            )

        if metadata1.week_number != metadata2.week_number:
            metadata_changes.append(
                FieldChange(
                    field_name="week_number",
                    old_value=metadata1.week_number,
                    new_value=metadata2.week_number,
                )
            )

        if metadata1.semester != metadata2.semester:
            metadata_changes.append(
                FieldChange(
                    field_name="semester",
                    old_value=metadata1.semester,
                    new_value=metadata2.semester,
                )
            )

        lessons1 = timetable1.lessons
        lessons2 = timetable2.lessons

        lessons1_map = {}
        for lesson in lessons1:
            key = Comparer._lesson_key(lesson)
            lessons1_map[key] = lesson

        lessons2_map = {}
        for lesson in lessons2:
            key = Comparer._lesson_key(lesson)
            lessons2_map[key] = lesson

        for key, lesson2 in lessons2_map.items():
            if key not in lessons1_map:
                lesson_changes.append(
                    LessonChange(
                        change_type=ChangeType.LESSON_ADDED,
                        field_changes=[
                            FieldChange(
                                field_name="lesson", old_value=None, new_value=lesson2
                            )
                        ],
                        old_lesson=None,
                        new_lesson=lesson2,
                    )
                )
            else:
                lesson1 = lessons1_map[key]
                field_changes = Comparer._compare_lessons(lesson1, lesson2)
                if field_changes:
                    lesson_changes.append(
                        LessonChange(
                            change_type=ChangeType.LESSON_MODIFIED,
                            field_changes=field_changes,
                            old_lesson=lesson1,
                            new_lesson=lesson2,
                        )
                    )

        for key, lesson1 in lessons1_map.items():
            if key not in lessons2_map:
                lesson_changes.append(
                    LessonChange(
                        change_type=ChangeType.LESSON_REMOVED,
                        field_changes=[
                            FieldChange(
                                field_name="lesson", old_value=lesson1, new_value=None
                            )
                        ],
                        old_lesson=lesson1,
                        new_lesson=None,
                    )
                )

        if not metadata_changes and not lesson_changes:
            return None

        return TimetableChangeData(
            entity=id_info,
            metadata_changes=metadata_changes,
            lesson_changes=lesson_changes,
        )

    @staticmethod
    def _lesson_key(lesson):
        return (
            lesson.schedule_type,
            lesson.time_begin,
            lesson.lesson_name,
            lesson.day_name,
        )

    @staticmethod
    @profile
    def _compare_lessons(lesson1, lesson2):
        field_changes = []

        for attr in [
            "lesson_type",
            "schedule_form",
            "duration",
            "auditorium",
            "location",
            "week_number",
            "subgroups",
        ]:
            val1 = getattr(lesson1, attr)
            val2 = getattr(lesson2, attr)
            if val1 != val2:
                field_changes.append(
                    FieldChange(field_name=attr, old_value=val1, new_value=val2)
                )

        for attr in ["professors", "groups"]:
            val1 = getattr(lesson1, attr)
            val2 = getattr(lesson2, attr)
            if val1 is None and val2 is None:
                continue
            if (
                val1 is None
                or val2 is None
                or set(val1 if val1 else []) != set(val2 if val2 else [])
            ):
                field_changes.append(
                    FieldChange(field_name=attr, old_value=val1, new_value=val2)
                )

        return field_changes
