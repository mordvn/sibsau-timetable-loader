import asyncio
from loguru import logger

from logger import configure_logging
from config import settings

from runner import Runner


async def main():
    logger.info("Starting")

    while True:
        try:
            await Runner.process_all_entities()
        except Exception as e:
            logger.exception(f"Error in main loop: {e}")
        await asyncio.sleep(settings.ENTITIES_FETCH_INTERVAL)


if __name__ == "__main__":
    configure_logging()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt")
    except Exception as e:
        logger.exception(f"Error when starting: {e}")
