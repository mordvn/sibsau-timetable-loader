from logger import trace
from profiler import profile
from parser_types import (
    Entity,
    TimetableData,
    WeekNumber,
    ScheduleType,
    DayName,
    LessonType,
    Subgroup,
    Lesson,
    Metadata,
    Semester,
)
from aiohttp import ClientSession
from bs4 import BeautifulSoup
import re
from datetime import date, time, timedelta


class Parser:
    @staticmethod
    @profile
    async def get_timetable(entity: Entity) -> TimetableData:
        html = await Parser._fetch_timetable(entity)
        return Parser._parse_timetable(html, entity)

    @staticmethod
    @profile
    async def _fetch_timetable(entity: Entity) -> str:
        async with ClientSession() as session:
            async with session.get(
                f"https://timetable.pallada.sibsau.ru/timetable/{entity.type.value}/{entity.id}"
            ) as response:
                return await response.text()

    @staticmethod
    @profile
    def _parse_timetable(html: str, entity: Entity) -> TimetableData:
        soup = BeautifulSoup(html, "html.parser")

        entity = Entity(
            type=entity.type, id=entity.id, name=Parser._parse_id_name(soup)
        )
        if entity.name == "":
            raise Exception(f"Failed to parse Timetable for {entity.type.value} {entity.id}")

        metadata = Parser._parse_metadata(soup)

        lessons = []

        regular_tab = soup.find("div", {"id": "timetable_tab", "class": "tab-pane"})
        if regular_tab:
            for week_tab in soup.find_all(
                "div", {"role": "tabpanel", "id": lambda x: x and x.startswith("week_")}
            ):
                week_id = week_tab.get("id", "")
                week_number = (
                    WeekNumber.ODD if "week_1_tab" in week_id else WeekNumber.EVEN
                )

                for day_div in week_tab.find_all(
                    "div", {"class": lambda x: x and "day" in x.split()}
                ):
                    day_name = Parser._extract_day_name(day_div)

                    for lesson_div in day_div.find_all("div", {"class": "line"}):
                        lesson_data = Parser._parse_lesson(
                            lesson_div, ScheduleType.REGULAR, day_name, week_number
                        )
                        if lesson_data:
                            lessons.extend(lesson_data)

        session_tab = soup.find("div", {"id": "session_tab", "class": "tab-pane"})
        if session_tab and not session_tab.find("div", {"class": "empty_info_msg"}):
            for day_div in session_tab.find_all(
                "div", {"class": lambda x: x and "day" in x.split()}
            ):
                day_name = Parser._extract_day_name(day_div)
                for lesson_div in day_div.find_all("div", {"class": "line"}):
                    lesson_data = Parser._parse_lesson(
                        lesson_div, ScheduleType.SESSION, day_name
                    )
                    if lesson_data:
                        lessons.extend(lesson_data)

        consultation_tab = soup.find(
            "div", {"id": "consultation_tab", "class": "tab-pane"}
        )
        if consultation_tab and not consultation_tab.find(
            "div", {"class": "empty_info_msg"}
        ):
            for day_div in consultation_tab.find_all(
                "div", {"class": lambda x: x and "day" in x.split()}
            ):
                day_name = Parser._extract_day_name(day_div)
                for lesson_div in day_div.find_all("div", {"class": "line"}):
                    lesson_data = Parser._parse_lesson(
                        lesson_div, ScheduleType.CONSULTATION, day_name
                    )
                    if lesson_data:
                        lessons.extend(lesson_data)

        return TimetableData(entity=entity, metadata=metadata, lessons=lessons)

    @staticmethod
    @profile
    def _parse_id_name(soup: BeautifulSoup) -> str:
        title_text = soup.find("title").text.strip() if soup.find("title") else ""

        name = ""
        if title_text and "Расписание" in title_text:
            name = title_text.replace("Расписание", "").strip()

        if not name:
            h3 = soup.find("h3", {"class": "text-center"})
            if h3:
                name = h3.text.strip().replace('"', "")

        return name

    @staticmethod
    @profile
    def _parse_metadata(soup: BeautifulSoup) -> Metadata:
        years = ""
        current_date = None
        week_number = WeekNumber.ODD
        semester = None

        h3 = soup.find("h3", {"class": "text-center"})
        if h3:
            text = h3.text.strip()
            if "семестр" in text and "-" in text:
                semester_text = text.split("семестр")[0].strip().split()[-1]
                semester = Semester.FIRST if semester_text == "1" else Semester.SECOND

                years_match = re.search(r"(\d{4}-\d{4})", text)
                if years_match:
                    years = years_match.group(1)
            else:
                # Check for professor format: "Алиева Д. П. - 2024/2025"
                years_match = re.search(r"(\d{4}/\d{4})", text)
                if years_match:
                    # Convert from "2024/2025" to "2024-2025" format
                    years = years_match.group(1).replace("/", "-")

        h4 = soup.find("h4", {"class": "text-center"})
        if h4:
            text = h4.text.strip()
            date_match = re.search(r"(\d{2}\.\d{2}\.\d{4})", text)
            if date_match:
                date_str = date_match.group(1)
                try:
                    day, month, year = map(int, date_str.split("."))
                    current_date = date(year, month, day)
                except Exception:
                    pass

            week_match = re.search(r"(\d)\s+неделя", text)
            if week_match:
                week_num = week_match.group(1)
                week_number = WeekNumber.ODD if week_num == "1" else WeekNumber.EVEN

        return Metadata(
            years=years, date=current_date, week_number=week_number, semester=semester
        )

    @staticmethod
    def _extract_day_name(day_div: BeautifulSoup) -> DayName:
        day_text = (
            day_div.find("div", {"class": "name"}).text.strip()
            if day_div.find("div", {"class": "name"})
            else ""
        )

        if "Понедельник" in day_text:
            return DayName.MONDAY
        elif "Вторник" in day_text:
            return DayName.TUESDAY
        elif "Среда" in day_text:
            return DayName.WEDNESDAY
        elif "Четверг" in day_text:
            return DayName.THURSDAY
        elif "Пятница" in day_text:
            return DayName.FRIDAY
        elif "Суббота" in day_text:
            return DayName.SATURDAY

        return DayName.MONDAY

    @staticmethod
    @profile
    def _parse_lesson(
        lesson_div, schedule_type, day_name, week_number=None
    ) -> list[Lesson]:
        lessons = []

        time_div = lesson_div.find("div", {"class": "time"})
        time_begin = None
        duration = None

        if time_div:
            time_text = time_div.text.strip()
            time_match = re.search(r"(\d{2}):(\d{2})-(\d{2}):(\d{2})", time_text)
            if time_match:
                hour_begin, minute_begin, hour_end, minute_end = map(
                    int, time_match.groups()
                )
                time_begin = time(hour_begin, minute_begin)

                _time_end = time(hour_end, minute_end)
                duration_seconds = (hour_end - hour_begin) * 3600 + (
                    minute_end - minute_begin
                ) * 60
                duration = timedelta(seconds=duration_seconds)

        discipline_div = lesson_div.find("div", {"class": "discipline"})
        if not discipline_div:
            return lessons

        columns = discipline_div.find_all(
            "div", {"class": lambda x: x and "col-md-" in x}
        )

        if len(columns) <= 1:
            lesson_info = Parser._extract_lesson_info(discipline_div)
            if lesson_info and time_begin:
                lesson = Lesson(
                    schedule_type=schedule_type,
                    time_begin=time_begin,
                    lesson_name=lesson_info.get("name", ""),
                    lesson_type=lesson_info.get("type"),
                    professors=lesson_info.get("professors"),
                    groups=lesson_info.get("groups"),
                    auditorium=lesson_info.get("auditorium"),
                    location=lesson_info.get("location"),
                    subgroups=lesson_info.get("subgroup", Subgroup.COMMON),
                    day_name=day_name,
                    week_number=week_number,
                    duration=duration,
                )
                lessons.append(lesson)
        else:
            for column in columns:
                lesson_info = Parser._extract_lesson_info(column)
                if lesson_info and time_begin:
                    lesson = Lesson(
                        schedule_type=schedule_type,
                        time_begin=time_begin,
                        lesson_name=lesson_info.get("name", ""),
                        lesson_type=lesson_info.get("type"),
                        professors=lesson_info.get("professors"),
                        groups=lesson_info.get("groups"),
                        auditorium=lesson_info.get("auditorium"),
                        location=lesson_info.get("location"),
                        subgroups=lesson_info.get("subgroup", Subgroup.COMMON),
                        day_name=day_name,
                        week_number=week_number,
                        duration=duration,
                    )
                    lessons.append(lesson)

        return lessons

    @staticmethod
    @profile
    def _extract_lesson_info(container) -> dict:
        result = {
            "name": "",
            "type": None,
            "professors": [],
            "groups": [],
            "auditorium": None,
            "location": None,
            "subgroup": Subgroup.COMMON,
        }

        subgroup_label = container.find("li", {"class": "bold num_pdgrp"})
        if subgroup_label:
            subgroup_text = subgroup_label.text.strip()
            if "1 подгруппа" in subgroup_text:
                result["subgroup"] = Subgroup.FIRST
            elif "2 подгруппа" in subgroup_text:
                result["subgroup"] = Subgroup.SECOND

        list_items = container.find_all("li")
        for item in list_items:
            text = item.text.strip()

            if item.find("span", {"class": "name"}):
                name_span = item.find("span", {"class": "name"})
                name = name_span.text.strip()

                if name:
                    result["name"] = name[0].upper() + name[1:].lower()

                type_match = re.search(r"\((.*?)\)", text)
                if type_match:
                    type_text = type_match.group(1).strip()
                    if type_text == "Лекция":
                        result["type"] = LessonType.LECTURE
                    elif type_text == "Практика":
                        result["type"] = LessonType.PRACTICE
                    elif type_text == "Лабораторная работа":
                        result["type"] = LessonType.LABORATORY
                    elif type_text == "Консультация":
                        result["type"] = LessonType.CONSULTATION
                    elif type_text == "Экзамен":
                        result["type"] = LessonType.EXAM

            if item.find("i", {"class": lambda x: x and "fa-user" in x}):
                professor_link = item.find("a")
                if professor_link:
                    professor_name = professor_link.text.strip()
                    result["professors"].append(professor_name)

            if item.find("i", {"class": lambda x: x and "fa-group" in x}):
                group_link = item.find("a")
                if group_link:
                    group_name = group_link.text.strip()
                    result["groups"].append(group_name)

            if item.find("i", {"class": lambda x: x and "fa-compass" in x}):
                auditorium_link = item.find("a")
                if auditorium_link:
                    auditorium_text = auditorium_link.text.strip()
                    location = auditorium_link.get("title", "")

                    if "корп." in auditorium_text and "каб." in auditorium_text:
                        building_match = re.search(
                            r'корп.\s*"([^"]+)"', auditorium_text
                        )
                        room_match = re.search(r'каб.\s*"([^"]+)"', auditorium_text)

                        if building_match and room_match:
                            building = building_match.group(1)
                            room = room_match.group(1)

                            result["auditorium"] = f"{building}-{room}"

                    result["location"] = location

            if item.find("i", {"class": lambda x: x and "fa-paperclip" in x}):
                if "1 подгруппа" in text:
                    result["subgroup"] = Subgroup.FIRST
                elif "2 подгруппа" in text:
                    result["subgroup"] = Subgroup.SECOND

        return result
