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
        default_semester = None
        for timetable in timetables:
            if timetable.metadata.semester:
                default_semester = timetable.metadata.semester
                break
                
        if default_semester:
            for timetable in timetables:
                if not timetable.metadata.semester:
                    timetable.metadata.semester = default_semester
                    
        return timetables
