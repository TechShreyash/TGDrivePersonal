root_folder = "/mnt/2b8b61d3-0169-469a-9a86-08ab209c93e5/Storage"  # Path of the local folder to upload
root_name = "Data"  # Name of the root folder in the TGDrive

import os
import sys
import asyncio
import time
from tqdm import tqdm
import os
import signal

from utils.logger import Logger
from config import BOT_TOKENS
from utils.clients import initialize_clients
from utils.directoryHandler import backup_drive_data, getRandomID
from utils.extra import convert_class_to_dict
from utils.uploader import start_file_uploader

logger = Logger("localManager")


# Get immediate subdirectories (not recursive) from the given folder.
def get_all_folders(root_folder):
    folders = [
        os.path.join(root_folder, d)
        for d in os.listdir(root_folder)
        if os.path.isdir(os.path.join(root_folder, d))
    ]
    return folders


# Get only the files in the given directory (no subdirectories).
def get_all_files(root_folder):
    files = [
        os.path.join(root_folder, f)
        for f in os.listdir(root_folder)
        if os.path.isfile(os.path.join(root_folder, f))
    ]
    return files


# Get cloud path for a folder with the given name under a given cloud parent path.
def getCpath(name, cparent):
    from utils.directoryHandler import DRIVE_DATA

    try:
        folder_data = DRIVE_DATA.get_directory(cparent)
        folder_data = convert_class_to_dict(folder_data, isObject=True, showtrash=False)
        for id, data in folder_data["contents"].items():
            if data["name"] == name:
                logger.debug(
                    f"Found existing file/folder '{name}' with id {id} under {cparent}"
                )
                return ("/" + data["path"] + id + "/").replace("//", "/")
    except Exception as e:
        logger.error(f"Exception in getCpath: {e}")
    return False


RUNNING_IDS = []
TOTAL_UPLOAD = 0

# Use an asyncio.Queue for upload tasks. Each task is a tuple:
# (file, id, cpath, fname, file_size, b)
upload_queue = asyncio.Queue()


async def worker():
    """Worker to process upload tasks from the queue."""
    while True:
        try:
            file, id, cpath, fname, file_size, b = await upload_queue.get()
        except asyncio.CancelledError:
            break

        logger.info(f"Uploading file: '{fname}' (ID: {id})")
        try:
            await start_file_uploader(file, id, cpath, fname, file_size, b)
        except Exception as e:
            with open("failed.txt", "a") as f:
                f.write(f"{file}\n")
            logger.error(f"Failed to upload '{fname}' (ID: {id}): {e}")
        from utils.uploader import PROGRESS_CACHE

        PROGRESS_CACHE[id] = ("completed", file_size, file_size)
        logger.info(f"Upload completed for file: '{fname}' (ID: {id})")
        upload_queue.task_done()


async def limited_uploader_progress():
    global RUNNING_IDS, TOTAL_UPLOAD
    logger.info(f"Total upload size: {TOTAL_UPLOAD} bytes")
    logger.info("Starting upload progress tracking")
    no_progress_counter = 0
    loop = asyncio.get_running_loop()
    with tqdm(
        total=TOTAL_UPLOAD,
        unit="B",
        unit_scale=True,
        desc="Uploading",
        dynamic_ncols=True,
        colour="green",
    ) as pbar:
        prev_done = 0
        while True:
            from utils.uploader import PROGRESS_CACHE

            done = 0
            complete = 0
            for id in RUNNING_IDS:
                x = PROGRESS_CACHE.get(id, ("running", 0, 0))
                done += x[1]
                if x[0] == "completed":
                    complete += 1
            delta = done - prev_done

            # Offload the blocking update to a thread.
            await loop.run_in_executor(None, pbar.update, delta)
            prev_done = done

            if complete == len(RUNNING_IDS):
                logger.info("All files have been uploaded successfully.")
                break

            if delta == 0:
                no_progress_counter += 1
            else:
                no_progress_counter = 0

            if no_progress_counter >= 30:
                logger.error(
                    "Upload progress seems to be stuck. Aborting progress tracking."
                )
                break

            await asyncio.sleep(5)
    logger.info("Upload progress tracking ended")


def isFailedFile(lpath):
    """Check if a file is marked as failed."""
    with open("failed.txt", "r") as f:
        failed_files = f.read().splitlines()
    return lpath in failed_files


async def start():
    if not os.path.exists("failed.txt"):
        with open("failed.txt", "w") as file:
            pass

    logger.info("Initializing clients...")
    await initialize_clients()

    DRIVE_DATA = None
    while not DRIVE_DATA:
        from utils.directoryHandler import DRIVE_DATA

        await asyncio.sleep(3)
        logger.debug("Waiting for DRIVE_DATA to be initialized...")

    max_concurrent_tasks = min(4, len(BOT_TOKENS))
    logger.info(f"Maximum concurrent upload tasks set to: {max_concurrent_tasks}")

    global RUNNING_IDS, TOTAL_UPLOAD

    # Schedule upload tasks for all files in a folder.
    def upload_files(lpath, cpath):
        global TOTAL_UPLOAD
        files = get_all_files(lpath)
        for file in files:
            if isFailedFile(file):
                logger.warning(f"Skipping previously failed file: {file}")
                continue

            fname = os.path.basename(file)
            # Check if the file already exists in the cloud.
            new_cpath = getCpath(fname, cpath)
            if new_cpath:
                logger.info(
                    f"File '{fname}' already exists in cloud at {new_cpath}. Skipping upload."
                )
                continue
            try:
                file_size = os.path.getsize(file)
            except Exception as e:
                with open("failed.txt", "a") as f:
                    f.write(f"{file}\n")
                logger.error(f"Failed to get size for file '{fname}': {e}")
                continue
            id = getRandomID()
            RUNNING_IDS.append(id)
            logger.debug(
                f"Enqueued file '{fname}' (ID: {id}) for upload to cloud path {cpath}"
            )
            TOTAL_UPLOAD += file_size
            # Enqueue the upload task. 'b' is set to False.
            upload_queue.put_nowait((file, id, cpath, fname, file_size, False))

    # Create the root folder in the cloud if it does not exist.
    root_cpath = getCpath(root_name, "/")
    if root_cpath:
        logger.info(
            f"Root folder '{root_name}' already exists in cloud at {root_cpath}"
        )
    else:
        logger.info(f"Creating root folder '{root_name}' in cloud")
        root_cpath = DRIVE_DATA.new_folder("/", root_name)
        logger.info(f"Root folder '{root_name}' created in cloud at {root_cpath}")

    # Upload files in the root local folder.
    upload_files(root_folder, root_cpath)

    # Recursively create folders and schedule file uploads.
    def create_folders(lpath, cpath):
        logger.debug(f"Processing local folder: {lpath}")
        folders = get_all_folders(lpath)
        for new_lpath in folders:
            folder_name: str = os.path.basename(new_lpath)
            if folder_name.startswith("."):
                logger.debug(f"Skipping hidden folder: {folder_name}")
                continue
            new_cpath = getCpath(folder_name, cpath)
            if not new_cpath:
                logger.info(
                    f"Creating cloud folder for local folder '{folder_name}' under {cpath}"
                )
                new_cpath = DRIVE_DATA.new_folder(cpath, folder_name)
            # Schedule uploads for files in the current folder.
            upload_files(new_lpath, new_cpath)
            # Recursively process subfolders.
            create_folders(new_lpath, new_cpath)
        logger.debug(f"Finished processing local folder: {lpath}")

    create_folders(root_folder, root_cpath)
    logger.info("All upload tasks have been scheduled. Waiting for completion...")

    # Start worker tasks.
    workers = [asyncio.create_task(worker()) for _ in range(max_concurrent_tasks)]
    progress_task = asyncio.create_task(limited_uploader_progress())

    # Wait until the upload queue is fully processed.
    await upload_queue.join()

    # Cancel worker tasks.
    for w in workers:
        w.cancel()
    await asyncio.gather(*workers, return_exceptions=True)

    await progress_task

    logger.info("All uploads completed successfully.")
    await backup_drive_data(False)
    logger.info("Backup completed successfully.")
    logger.info("Exiting...")
    await asyncio.sleep(1)

    # Forcefully terminates the program immediately
    os.kill(os.getpid(), signal.SIGKILL)


if __name__ == "__main__":
    asyncio.run(start())
