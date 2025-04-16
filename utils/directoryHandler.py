from pathlib import Path
import sys
import config, dill
from pyrogram.types import InputMediaDocument, Message
import os, random, string, asyncio
from utils.logger import Logger
from datetime import datetime, timezone
import os
import signal

logger = Logger(__name__)

cache_dir = Path("./cache")
cache_dir.mkdir(parents=True, exist_ok=True)
drive_cache_path = cache_dir / "drive.data"


def getRandomID():
    global DRIVE_DATA
    while True:
        id = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
        if not DRIVE_DATA:
            return id
        if id not in DRIVE_DATA.used_ids:
            DRIVE_DATA.used_ids.append(id)
            return id


def get_current_utc_time():
    return datetime.now(timezone.utc).strftime("Date - %Y-%m-%d | Time - %H:%M:%S")


class Folder:
    def __init__(self, name: str, path: str) -> None:
        self.name = name
        self.contents = {}
        if name == "/":
            self.id = "root"
        else:
            self.id = getRandomID()
        self.type = "folder"
        self.trash = False
        self.path = ("/" + path.strip("/") + "/").replace("//", "/")
        self.upload_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.auth_hashes = []


class File:
    def __init__(
        self,
        name: str,
        file_id: int,
        size: int,
        path: str,
    ) -> None:
        self.name = name
        self.file_id = file_id
        self.id = getRandomID()
        self.size = size
        self.type = "file"
        self.trash = False
        self.path = path[:-1] if path[-1] == "/" else path
        self.upload_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class NewDriveData:
    def __init__(self, contents: dict, used_ids: list) -> None:
        self.contents = contents
        self.used_ids = used_ids
        self.isUpdated = False

    def save(self) -> None:
        with open(drive_cache_path, "wb") as f:
            dill.dump(self, f)
        self.isUpdated = True
        logger.info("Drive data saved successfully.")

    def new_folder(self, path: str, name: str) -> None:
        logger.info(f"Creating new folder '{name}' in path '{path}'.")

        folder = Folder(name, path)
        if path == "/":
            directory_folder: Folder = self.contents[path]
            directory_folder.contents[folder.id] = folder
        else:
            paths = path.strip("/").split("/")
            directory_folder: Folder = self.contents["/"]
            for path in paths:
                directory_folder = directory_folder.contents[path]
            directory_folder.contents[folder.id] = folder

        self.save()
        return folder.path + folder.id

    def new_file(self, path: str, name: str, file_id: int, size: int) -> None:
        logger.info(f"Creating new file '{name}' in path '{path}'.")

        file = File(name, file_id, size, path)
        if path == "/":
            directory_folder: Folder = self.contents[path]
            directory_folder.contents[file.id] = file
        else:
            paths = path.strip("/").split("/")
            directory_folder: Folder = self.contents["/"]
            for path in paths:
                directory_folder = directory_folder.contents[path]
            directory_folder.contents[file.id] = file

        self.save()

    def get_directory(
        self, path: str, is_admin: bool = True, auth: str = None
    ) -> Folder:
        folder_data: Folder = self.contents["/"]
        auth_success = False
        auth_home_path = None

        if path != "/":
            path = path.strip("/")

            if "/" in path:
                path = path.split("/")
            else:
                path = [path]

            for folder in path:
                folder_data = folder_data.contents[folder]

                if auth in folder_data.auth_hashes:
                    auth_success = True
                    auth_home_path = (
                        "/" + folder_data.path.strip("/") + "/" + folder_data.id
                    )

        if not is_admin and not auth_success:
            logger.warning(f"Unauthorized access attempt to path '{path}'.")
            return None

        if auth_success:
            logger.info(f"Authorization successful for path '{path}'.")
            return folder_data, auth_home_path

        return folder_data

    def get_folder_auth(self, path: str) -> None:
        auth = getRandomID()
        folder_data: Folder = self.contents["/"]

        if path != "/":
            path = path.strip("/")

            if "/" in path:
                path = path.split("/")
            else:
                path = [path]

            for folder in path:
                folder_data = folder_data.contents[folder]

        folder_data.auth_hashes.append(auth)
        self.save()
        logger.info(f"Authorization hash generated for path '{path}'.")
        return auth

    def get_file(self, path) -> File:
        if len(path.strip("/").split("/")) > 0:
            folder_path = "/" + "/".join(path.strip("/").split("/")[:-1])
            file_id = path.strip("/").split("/")[-1]
        else:
            folder_path = "/"
            file_id = path.strip("/")

        folder_data = self.get_directory(folder_path)
        return folder_data.contents[file_id]

    def rename_file_folder(self, path: str, new_name: str) -> None:
        if len(path.strip("/").split("/")) > 0:
            folder_path = "/" + "/".join(path.strip("/").split("/")[:-1])
            file_id = path.strip("/").split("/")[-1]
        else:
            folder_path = "/"
            file_id = path.strip("/")
        folder_data = self.get_directory(folder_path)
        folder_data.contents[file_id].name = new_name
        self.save()
        logger.info(f"Item at path '{path}' renamed to '{new_name}'.")

    def trash_file_folder(self, path: str, trash: bool) -> None:
        action = "Trashing" if trash else "Restoring"

        if len(path.strip("/").split("/")) > 0:
            folder_path = "/" + "/".join(path.strip("/").split("/")[:-1])
            file_id = path.strip("/").split("/")[-1]
        else:
            folder_path = "/"
            file_id = path.strip("/")
        folder_data = self.get_directory(folder_path)
        folder_data.contents[file_id].trash = trash
        self.save()
        logger.info(f"Item at path '{path}' {action.lower()} successfully.")

    def get_trashed_files_folders(self):
        root_dir = self.get_directory("/")
        trash_data = {}

        def traverse_directory(folder):
            for item in folder.contents.values():
                if item.type == "folder":
                    if item.trash:
                        trash_data[item.id] = item
                    else:
                        # Recursively traverse the subfolder
                        traverse_directory(item)
                elif item.type == "file":
                    if item.trash:
                        trash_data[item.id] = item

        traverse_directory(root_dir)
        return trash_data

    def delete_file_folder(self, path: str) -> None:

        if len(path.strip("/").split("/")) > 0:
            folder_path = "/" + "/".join(path.strip("/").split("/")[:-1])
            file_id = path.strip("/").split("/")[-1]
        else:
            folder_path = "/"
            file_id = path.strip("/")

        folder_data = self.get_directory(folder_path)
        del folder_data.contents[file_id]
        self.save()
        logger.info(f"Item at path '{path}' deleted successfully.")

    def search_file_folder(self, query: str):
        logger.info(f"Searching for items matching query '{query}'.")

        root_dir = self.get_directory("/")
        search_results = {}

        def traverse_directory(folder):
            for item in folder.contents.values():
                if query.lower() in item.name.lower():
                    search_results[item.id] = item
                if item.type == "folder":
                    traverse_directory(item)

        traverse_directory(root_dir)
        logger.info(f"Search completed. Found {len(search_results)} matching items.")
        return search_results


class NewBotMode:
    def __init__(self, drive_data: NewDriveData) -> None:
        self.drive_data = drive_data

        # Set the current folder to root directory by default
        self.current_folder = "/"
        self.current_folder_name = "/ (root directory)"

    def set_folder(self, folder_path: str, name: str) -> None:
        self.current_folder = folder_path
        self.current_folder_name = name
        self.drive_data.save()
        logger.info(f"Current folder set to '{name}' at path '{folder_path}'.")


DRIVE_DATA: NewDriveData = None
BOT_MODE: NewBotMode = None


# Function to backup the drive data to telegram
async def backup_drive_data(loop=True):
    global DRIVE_DATA
    logger.info("Starting backup drive data task.")

    while True:
        try:
            if not DRIVE_DATA.isUpdated:
                if not loop:
                    break
                await asyncio.sleep(config.DATABASE_BACKUP_TIME)
                continue

            logger.info("Backing up drive data to Telegram.")
            from utils.clients import get_client

            client = get_client()
            time_text = f"📅 **Last Updated :** {get_current_utc_time()} (UTC +00:00)"
            caption = (
                f"🔐 **TG Drive Data Backup File**\n\n"
                "Do not edit or delete this message. This is a backup file for the tg drive data.\n\n"
                f"{time_text}"
            )

            media_doc = InputMediaDocument(drive_cache_path, caption=caption)
            msg = await client.edit_message_media(
                config.STORAGE_CHANNEL,
                config.DATABASE_BACKUP_MSG_ID,
                media=media_doc,
                file_name="drive.data",
            )

            DRIVE_DATA.isUpdated = False
            logger.info("Drive data backed up to Telegram successfully.")

            try:
                await msg.pin()
            except Exception as pin_e:
                logger.error(f"Error pinning backup message: {pin_e}")

            if not loop:
                break

            await asyncio.sleep(config.DATABASE_BACKUP_TIME)
        except Exception as e:
            logger.error(f"Backup Error: {e}")
            await asyncio.sleep(10)


async def init_drive_data():
    global DRIVE_DATA

    logger.info("Initializing drive data.")
    root_dir = DRIVE_DATA.get_directory("/")
    if not hasattr(root_dir, "auth_hashes"):
        root_dir.auth_hashes = []

    def traverse_directory(folder):
        for item in folder.contents.values():
            if item.type == "folder":
                traverse_directory(item)

                if not hasattr(item, "auth_hashes"):
                    item.auth_hashes = []

    traverse_directory(root_dir)
    DRIVE_DATA.save()
    logger.info("Drive data initialization completed.")


async def loadDriveData():
    global DRIVE_DATA, BOT_MODE

    logger.info("Loading drive data.")
    from utils.clients import get_client

    client = get_client()
    try:
        try:
            msg: Message = await client.get_messages(
                config.STORAGE_CHANNEL, config.DATABASE_BACKUP_MSG_ID
            )
        except Exception as e:
            logger.error(f"Error fetching backup message: {e}")

            # Forcefully terminates the program immediately
            os.kill(os.getpid(), signal.SIGKILL)

        if not msg.document:
            logger.error(f"Error fetching backup message: {e}")

            # Forcefully terminates the program immediately
            os.kill(os.getpid(), signal.SIGKILL)

        if msg.document.file_name == "drive.data":
            dl_path = await msg.download()
            with open(dl_path, "rb") as f:
                DRIVE_DATA = dill.load(f)

            logger.info("Drive data loaded from Telegram backup.")
        else:
            raise Exception("Backup drive.data file not found on Telegram.")
    except Exception as e:
        logger.warning(f"Backup load failed: {e}")
        logger.info("Creating new drive.data file.")
        DRIVE_DATA = NewDriveData({"/": Folder("/", "/")}, [])
        DRIVE_DATA.save()

    await init_drive_data()

    if config.MAIN_BOT_TOKEN:
        from utils.bot_mode import start_bot_mode

        BOT_MODE = NewBotMode(DRIVE_DATA)
        await start_bot_mode(DRIVE_DATA, BOT_MODE)
        logger.info("Bot mode started.")
