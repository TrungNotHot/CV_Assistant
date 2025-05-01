# Job Crawler for ITviec & VietnamWorks

This project provides a Python-based job crawler that extracts recruitment data from two major platforms: [ITviec](https://itviec.com) and [VietnamWorks](https://vietnamworks.com). All crawled data is stored in MongoDB.

---

## 💾 Requirements

### 1. Python & Virtual Environment

```bash
cd data/crawlers/
python3 -m venv venv
source venv/bin/activate  # For Windows: venv\Scripts\activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

---

## 🔧 Environment Variables

Create a `.env` file in the root directory by copying the provided example:

```bash
cp .env.example .env
```

Then fill in the following fields in `.env`:

```env
MONGO_URI=your_mongo_connection_string
MONGO_DB=crawl-jd
MONGO_COLLECTION=test
```

---

## 🚀 Running the Crawlers

### 1. Crawl ITviec data

```bash
xvfb-run -a python ./src/itviec_crawl.py
```

### 2. Crawl VietnamWorks data

```bash
xvfb-run -a python ./src/vietnamwork_crawl.py
```

### 3. Crawl Topdev data

```bash
xvfb-run -a python ./src/topdev_crawl.py
```

---

## 🔁 Resume Crawling from a Specific Page

If the crawler is interrupted or fails at a specific page, resume from that page using the `--start-page` option.

### Resume ITviec from page 2:

```bash
xvfb-run -a python ./src/itviec_crawl.py --start-page 2
```

### Resume VietnamWorks from page 2:

```bash
xvfb-run -a python ./src/vietnamwork_crawl.py --start-page 2
```

> ⚠️ Note: `--start-page` is 1-based (i.e., page number as shown in the browser), not API-indexed.

---

## 🧾 MongoDB Data Structure

- **Database**: as specified in `MONGO_DB`
- **Collection**: as specified in `MONGO_COLLECTION`
- **Unique Key**: `hash`

---

## 📁 Project Structure

```
crawl-jd/
├── .env.example
├── .env
├── .gitignore
├── README.md
├── requirements.txt
├── src/
│   ├── itviec_crawl.py
│   ├── vietnamwork_crawl.py
│   ├── topdev_crawl.py
│   └── mongo_config.py
└── ...
```

---

## ✅ Setup Guide for New Developers

Follow these steps right after cloning or pulling the project to get started:

### 1. Navigate to the project directory

```bash
cd data/crawlers/
```

### 2. Create and activate Python virtual environment

```bash
python3 -m venv venv
source venv/bin/activate  # (Windows: venv\Scripts\activate)
```

### 3. Install required libraries

```bash
pip install -r requirements.txt
```

### 4. Create environment configuration file

```bash
cp .env.example .env
```

Then edit `.env` with your MongoDB settings:

```env
MONGO_URI=mongodb+srv://...
MONGO_DB=crawl-data
MONGO_COLLECTION=jd
```

### 5. (Optional) Install xvfb if on a Linux server

```bash
sudo apt install xvfb
```

### 6. Run the crawler

#### ITviec:
```bash
xvfb-run -a python ./src/itviec_crawl.py
```

#### VietnamWorks:
```bash
xvfb-run -a python ./src/vietnamwork_crawl.py
```

### 7. (Optional) Resume on failure

```bash
xvfb-run -a python ./src/itviec_crawl.py --start-page 5
xvfb-run -a python ./src/vietnamwork_crawl.py --start-page 7
```

---

## ⚠ Python 3.12+ Notes

Python 3.12 removed `distutils`. If you encounter this error:

```
ModuleNotFoundError: No module named 'distutils'
```

Please run:

```bash
pip install setuptools
```

Ensure your `requirements.txt` includes:

```txt
setuptools>=65.5.0
```

---

## 📌 Notes

- `xvfb-run` is used to run headless browsers on systems without a graphical interface (e.g., Linux servers).
- The script automatically **skips duplicates** using the `hash` field as a unique identifier.

