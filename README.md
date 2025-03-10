# TG Drive Personal

TG Drive Personal is an enhanced fork of [TG Drive](https://github.com/TechShreyash/TGDrive) that lets you easily back up your local desktop files to TGDrive. With this tool, you can store your files in Telegram (using private channels) and later access them anytime via the TGDrive UI. In addition to the original functionality, this project introduces a new script, [localManager.py](localManager.py), to manage the upload process while preserving your local directory structure.

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Setup and Configuration](#setup-and-configuration)
- [Usage](#usage)
- [Why Use TGDrive?](#why-use-tgdrive)
- [License](#license)
- [Support](#support)

---

## Overview

TG Drive Personal simplifies the process of backing up your local files to Telegram. By leveraging the secure storage of Telegram's private channels, you can maintain an organized backup of your important data without compromising security. The project preserves the local directory structure in TGDrive, ensuring a seamless experience when browsing your files later.

---

## Features

- **Local Backup:** Easily upload entire local directories to TGDrive.
- **Directory Preservation:** Maintains the same directory structure as your local files.
- **Simple Configuration:** Update just a couple of settings in [localManager.py](localManager.py) to get started.
- **Secure Storage:** Uses Telegram’s private channels, ensuring a safe and reliable backup method.

---

## Setup and Configuration

1. **Set Up Environment Variables:**  
   Follow the guide on [TG Drive's README](https://github.com/TechShreyash/TGDrive?tab=readme-ov-file#environment-variables) to configure the necessary environment variables.

2. **Configure localManager.py:**  
   Open [localManager.py](localManager.py) and locate the variables at the top of the file:
   - **`root_folder`:** Set this to the path of the local folder you want to upload.
   - **`root_name`:** Specify the name of the root folder in TGDrive where the files will be stored.

---

## Usage

To back up a local folder to TGDrive:

1. **Prepare Your Environment:**  
   Ensure that the TGDrive UI is not running while you perform the backup.

2. **Run the Script:**  
   Execute the script with the following command:
   ```bash
   python localManager.py
   ```
   The script will automatically upload all files from the specified local folder to TGDrive, preserving the original directory structure.

> **Important:**  
> - Do not run multiple instances of [localManager.py](localManager.py) simultaneously.  
> - Ensure the TGDrive UI is closed during the upload to avoid conflicts.

---

## Why Use TGDrive?

Telegram's private channels offer a secure method to store files, making it a viable backup solution as long as you comply with Telegram’s guidelines. TG Drive simplifies the upload process and provides an intuitive UI to browse and manage your backed-up files at any time.

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

## Support

For support or inquiries, please join our [Telegram Support Group](https://telegram.me/TechZBots_Support) or email [techshreyash123@gmail.com](mailto:techshreyash123@gmail.com).
