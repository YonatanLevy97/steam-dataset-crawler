# SteamCharts Crawler Refactored

מערכת מודולרית לחילוץ נתוני שחקנים מ-SteamCharts עם תמיכה בעיבוד batch, checkpoint recovery וניהול מתקדם של נתונים.

## תכונות עיקריות

- 🎮 **חילוץ נתוני שחקנים**: מושך נתונים חודשיים של שחקנים מ-SteamCharts
- 📦 **עיבוד Batch**: חלוקה אוטומטית של datasets גדולים לbatches קטנים יותר  
- 💾 **Checkpoint Recovery**: המשכת עבודה מנקודת עצירה אחרי תקלות
- 🔄 **Rate Limiting**: שליטה בקצב הבקשות למניעת blocking
- 📊 **ייצוא CSV**: שמירת נתונים בפורמט CSV עם העמודות הנדרשות
- 🛠️ **מבנה מודולרי**: ארכיטקטורה נקייה וקלה לתחזוקה

## דרישות מערכת

```bash
pip install requests beautifulsoup4
```

## מבנה הנתונים

הכלי מייצא נתונים עם העמודות הבאות:
- `appid`: מזהה המשחק ב-Steam
- `name`: שם המשחק  
- `month`: חודש (או "Last 30 Days")
- `avg_players`: ממוצע שחקנים
- `peak_players`: שיא שחקנים
- `change_percent`: אחוז שינוי מהחודש הקודם

## דוגמאות שימוש

### 1. חילוץ נתונים למשחק יחיד

```bash
cd examples
python crawl_single_game_players.py 730 "Counter-Strike 2"
```

### 2. יצירת batches מקובץ metadata

```bash
cd examples

# יצירת batches של 1000 משחקים כל אחד
python batch_manager_example.py create ../data_new/steam_app_metadata.csv 1000

# הצגת מידע על batches קיימים  
python batch_manager_example.py info
```

### 3. הרצת batch עם checkpoint recovery

```bash
cd examples

# הרצת batch מספר 1
python crawl_batch_players.py ../data_new/steam_app_metadata.csv 1

# אם הקוד נעצר, הרצה נוספת תמשיך מאותה נקודה
python crawl_batch_players.py ../data_new/steam_app_metadata.csv 1
```

### 4. שימוש בקוד (Programmatic Usage)

```python
from steamcharts_crawler_refactored import SteamChartsCrawler, DataExporter

# יצירת crawler
crawler = SteamChartsCrawler(delay_range=(0.3, 0.5))

# חילוץ נתונים למשחק
player_data = crawler.crawl_app_players(730, "Counter-Strike 2")

# שמירה ל-CSV
exporter = DataExporter()
exporter.save_to_csv(player_data, "cs2_players.csv")

crawler.close()
```

## תצורה מתקדמת

### הגדרות ברירת מחדל

```python
# config/settings.py
DEFAULT_DELAY_RANGE = (0.3, 0.5)  # עיכוב בין בקשות
DEFAULT_BATCH_SIZE = 1000          # גודל batch
DEFAULT_CHECKPOINT_INTERVAL = 100  # שמירת checkpoint כל N משחקים
```

### התאמת הגדרות

```python
from steamcharts_crawler_refactored.utils import BatchManager, CheckpointManager

# batch manager מותאם אישית
batch_mgr = BatchManager("metadata.csv", batch_size=500)

# checkpoint manager עם batch ID מותאם
checkpoint_mgr = CheckpointManager("output/", "my_batch")
```

## מבנה הפרויקט

```
steamcharts_crawler_refactored/
├── core/                    # רכיבי ליבה
│   ├── steamcharts_crawler.py
│   └── web_client.py
├── extractors/              # מחלצי נתונים
│   └── player_data_extractor.py
├── utils/                   # כלי עזר
│   ├── batch_manager.py
│   ├── checkpoint_manager.py
│   ├── data_exporter.py
│   └── logging_config.py
├── examples/                # דוגמאות שימוש
├── config/                  # הגדרות
└── data/                    # תיקיית פלט
```

## טיפול בשגיאות

- **Rate Limiting**: הכלי מטפל אוטומטית בעיכובים ו-retries
- **Checkpoint Recovery**: בקרת קובץ checkpoint.json לשחזור עבודה
- **Logging**: לוגים מפורטים לקובץ steamcharts_crawler.log
- **Error Handling**: המשכה גם אחרי שגיאות במשחקים בודדים

## עבודה עם חוקרים

### חלוקת עבודה

1. צור batches עם `batch_manager_example.py`
2. חלק מספרי batches בין חוקרים שונים
3. כל חוקר מריץ: `python crawl_batch_players.py metadata.csv <batch_id>`
4. איחוד קבצי CSV בסוף

### דוגמה לחלוקה בין 3 חוקרים

```bash
# חוקר 1: batches 1-10
for i in {1..10}; do python crawl_batch_players.py metadata.csv $i; done

# חוקר 2: batches 11-20  
for i in {11..20}; do python crawl_batch_players.py metadata.csv $i; done

# חוקר 3: batches 21-30
for i in {21..30}; do python crawl_batch_players.py metadata.csv $i; done
```

## הפרשים מהקוד המקורי

1. **מקור נתונים**: קריאה מקובץ metadata במקום Steam API
2. **Checkpoint Recovery**: שמירה אוטומטית והמשכה מנקודת עצירה  
3. **מבנה מודולרי**: חלוקה לרכיבים נפרדים
4. **Batch Processing**: תמיכה בעיבוד datasets גדולים
5. **Error Handling**: טיפול משופר בשגיאות

## ביצועים

- ~0.3-0.5 שניות בין בקשות (ניתן להתאמה)
- Checkpoint כל 100 משחקים (ברירת מחדל)  
- Batch של 1000 משחקים (ברירת מחדל)
- תמיכה בהרצה מקבילה של batches שונים
