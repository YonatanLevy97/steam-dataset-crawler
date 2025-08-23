# 🚀 START HERE - Steam Research Project

## 📋 What's in this folder:

```
steam_research_final/
├── START_HERE.md           ← YOU ARE HERE! Read this first
├── create_batches.py       ← Step 1: Run this once
├── research_crawler.py     ← Step 2: Each researcher edits & runs this
├── README_ENGLISH.md       ← Complete documentation
├── steam_crawler_refactored/ ← Core engine (don't touch)
└── app_id_batches/         ← 6 CSV files created by step 1
```

---

## ⚡ Quick Start (2 Steps Only!)

### Step 1: Create batches (run once by anyone)
```bash
python create_batches.py
```
**Result**: Creates 6 CSV files with ~42K App IDs each

### Step 2: Each researcher runs their batch
1. **Edit line 14** in `research_crawler.py`:
   ```python
   CSV_FILE_PATH = "app_id_batches/batch_X_apps.csv"  # Change X to 1,2,3,4,5,6
   ```

2. **Run it**:
   ```bash
   python research_crawler.py
   ```

**That's it!** Each researcher gets their own CSV file with results.

---

## 👥 Researcher Assignment

| Researcher | Edit this line in research_crawler.py |
|------------|---------------------------------------|
| **Researcher 1** | `CSV_FILE_PATH = "app_id_batches/batch_1_apps.csv"` |
| **Researcher 2** | `CSV_FILE_PATH = "app_id_batches/batch_2_apps.csv"` |
| **Researcher 3** | `CSV_FILE_PATH = "app_id_batches/batch_3_apps.csv"` |
| **Researcher 4** | `CSV_FILE_PATH = "app_id_batches/batch_4_apps.csv"` |
| **Researcher 5** | `CSV_FILE_PATH = "app_id_batches/batch_5_apps.csv"` |
| **Researcher 6** | `CSV_FILE_PATH = "app_id_batches/batch_6_apps.csv"` |

---

## 🎯 Expected Results

### Data collected for each game:
- **Basic info**: Name, developer, publisher, release date
- **Classification**: Genres, categories, tags
- **Pricing**: Free/paid, prices, discounts
- **Quality**: Metacritic scores, achievements, recommendations
- **Technical**: Platform support, system requirements
- **Content**: DLC count, age rating

### Output:
- **Live CSV**: Updates immediately (safe from crashes)
- **26 columns** of data per game
- **Automatic resume** if interrupted
- **~42K games per researcher** = 251K total Steam games

---

## ⏱️ Time Estimates

| Scenario | Per Researcher | All 6 Together |
|----------|----------------|-----------------|
| **Testing** (100 games) | 5 minutes | 5 minutes |
| **Sample** (1000 games) | 45 minutes | 45 minutes |
| **Full dataset** (42K games) | 2.2 days | 2.2 days |

---

## 🔧 Optional Settings

### For testing (edit in research_crawler.py):
```python
MAX_APPS = 100  # Only process 100 games
```

### For faster speed (higher risk):
```python
DELAY_RANGE = (1, 3)  # Faster but riskier
```

### For safer speed:
```python
DELAY_RANGE = (5, 10)  # Slower but safer
```

---

## 🆘 Troubleshooting

### "File not found" error:
```bash
python create_batches.py  # Run step 1 first
```

### Want to test first:
```python
MAX_APPS = 10  # Test with just 10 games
```

### Process interrupted:
```bash
python research_crawler.py  # Just run again - it resumes automatically
```

---

## 📊 Sample Output

```bash
🎮 Research Steam Crawler
   📁 App IDs file: app_id_batches/batch_1_apps.csv
   💾 Results file: results_batch_1_apps_20250823_163729.csv
   ⏱️  Delay: 3-6 seconds

[    1/42892] App 220: Half-Life 2
     ✅ 💰 📦1 ⭐96

🎯 Summary:
   ✅ Successful: 42,890
   ❌ Failed: 2
   📈 Success rate: 99.9%
   ⏱️  Total time: 3024.5 minutes
   🚀 Speed: 14.2 applications/minute
   💾 Results in: results_batch_1_apps_20250823_163729.csv
```

---

## 🎓 For Academic Use

This system collects comprehensive data on **all 251,355 Steam applications** for research on "What Causes Dead Games?" 

**Next steps after data collection**:
1. Combine all 6 CSV files
2. Add player count data (SteamCharts API)
3. Add review sentiment (Steam Reviews API)
4. Analyze correlation with "dead game" patterns

---

## 📞 Need Help?

1. **Read**: `README_ENGLISH.md` for complete documentation
2. **Test**: Set `MAX_APPS = 10` for quick testing
3. **Monitor**: Check `results_*.csv` files for progress

**Ready to collect the largest Steam dataset ever assembled for research!** 🎯

---

## ✅ Checklist Before Starting

- [ ] Python 3.7+ installed
- [ ] Internet connection stable
- [ ] ~2GB free disk space
- [ ] Ran `python create_batches.py`
- [ ] Edited `CSV_FILE_PATH` in `research_crawler.py`
- [ ] Ready to run `python research_crawler.py`

**Let's revolutionize game research!** 🚀
