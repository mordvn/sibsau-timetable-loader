from parser_types import (
    TimetableData,
)
from typing import List
from profiler import profile


class Validator:
    @staticmethod
    @profile(func_name="validator.validate_timetable")
    async def validate_timetables(
        timetables: List[TimetableData],
    ) -> List[TimetableData]:
        semester = None
        for timetable in timetables:
            if not semester:
                semester = timetable.metadata.semester
                break
        for timetable in timetables:
            if not timetable.metadata.semester:
                timetable.metadata.semester = semester
        return timetables
