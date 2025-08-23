# SteamCharts Research Crawler 📊

## 🎯 מה זה?

קובץ מרכזי פשוט לחילוץ נתוני שחקנים מ-SteamCharts עבור חוקרים. 
**כל חוקר צריך רק לשנות משתנה אחד ולהריץ!**

## 🚀 איך להשתמש?

### 1. פתח את הקובץ `steamcharts_research_crawler.py`

### 2. שנה רק את השורה הזאת:
```python
CSV_FILE_PATH = "app_id_batches/batch_5_apps.csv"
```

### 3. הרץ:
```bash
python steamcharts_research_crawler.py
```

**זהו! זה הכל!** 🎉

## 📁 מבנה הקבצים

```
project/
├── steamcharts_research_crawler.py  ← הקובץ המרכזי
├── app_id_batches/                  ← תיקיית הbatches
│   ├── batch_1_apps.csv
│   ├── batch_2_apps.csv
│   ├── batch_5_apps.csv             ← דוגמה
│   └── ...
└── steamcharts_results_batch_X.csv  ← קבצי תוצאות
```

## 🔧 הגדרות נוספות (אופציונלי)

```python
# Additional settings (optional)
DELAY_RANGE = (0.3, 0.5)  # Delay between requests (seconds)
MAX_APPS = None           # Limit for testing (None = all)
RESULTS_CSV = None        # Results file name (None = automatic)
```

## 📊 פורמט הנתונים

הכלי מייצר CSV עם העמודות:
- `appid`: מזהה המשחק ב-Steam
- `name`: שם המשחק
- `month`: חודש או "Last 30 Days"  
- `avg_players`: ממוצע שחקנים
- `peak_players`: שיא שחקנים
- `change_percent`: אחוז שינוי מהחודש הקודם
- `crawl_timestamp`: זמן החילוץ
- `crawl_status`: סטטוס (success/failed/no_data)

## 🔄 המשכה אוטומטית

- אם הקוד נעצר (תקלה/הפסקה), פשוט הרץ שוב
- המערכת תמשיך מהנקודה שנעצרה ✅
- כל שורה נכתבת מיד ל-CSV (בטוח מקריסות) 💾

## 📈 מה תראה במהלך הריצה?

```
📊 SteamCharts Research Crawler
   📁 App IDs file: app_id_batches/batch_5_apps.csv
   💾 Results file: steamcharts_results_batch_5.csv
   ⏱️  Delay: 0.3-0.5 seconds
   🆕 Fresh start: הרצה חדשה
==================================================

[    1/ 1000] App 730: Counter-Strike 2
     ✅ 158 monthly records
[    2/ 1000] App 570: Dota 2  
     ✅ 158 monthly records
...
```

## 🎯 דוגמה לחלוקת עבודה

### חוקר 1:
```python
CSV_FILE_PATH = "app_id_batches/batch_1_apps.csv"
```

### חוקר 2:
```python
CSV_FILE_PATH = "app_id_batches/batch_2_apps.csv"
```

### חוקר 3:
```python
CSV_FILE_PATH = "app_id_batches/batch_3_apps.csv"
```

## ✅ תכונות מתקדמות

- **📦 Live CSV Updates**: כל רשומה נכתבת מיד
- **🔄 Resume Capability**: המשכה אוטומטית אחרי הפסקות  
- **📊 Progress Tracking**: מעקב התקדמות ברור
- **⏱️  Time Estimation**: הערכת זמן סיום
- **💾 Checkpoint System**: שמירות ביניים
- **🛡️  Error Handling**: טיפול בשגיאות

## 🆘 פתרון בעיות

### "File does not exist!"
```bash
# וודא שהנתיב נכון
ls app_id_batches/
```

### המשכה אחרי הפסקה
```bash
# פשוט הרץ שוב - המערכת תמשיך אוטומטית
python steamcharts_research_crawler.py
```

### בדיקת התקדמות
```bash
# ספור שורות בקובץ התוצאות
wc -l steamcharts_results_batch_5.csv
```

## 📞 עזרה

אם יש בעיה, בדוק:
1. ✅ הנתיב לקובץ הbatch נכון?
2. ✅ יש אינטרנט?
3. ✅ הקובץ batch_X_apps.csv קיים?

**זהו! פשוט ומהיר! 🚀**
