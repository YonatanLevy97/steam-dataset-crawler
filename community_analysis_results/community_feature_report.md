# Steam Game Community Feature Analysis

## Overview
This report analyzes **4 communities** discovered through Louvain community detection on the Steam game dataset.

Each community represents a cluster of similar games based on cosine similarity of their features.

---

## Table of Contents

- [Community 0](#community-0) - Action games (Valve)
- [Community 1](#community-1) - Early Access games (Valve)
- [Community 2](#community-2) - Action games (THQ Nordic)
- [Community 3](#community-3) - Action games (Crystal Dynamics)

---

## Community 0
**Size:** 74 games

### 🎮 Top Genres

- **Action**: 44.59% (33 games)
- **Indie**: 41.89% (31 games)
- **Simulation**: 4.05% (3 games)
- **Casual**: 4.05% (3 games)
- **Adventure**: 35.14% (26 games)

### 🏢 Top Publishers

- **Valve**: 9.46% (7 games)
- **SEGA**: 6.76% (5 games)
- **Bethesda Softworks**: 5.41% (4 games)
- **2K**: 5.41% (4 games)
- **Mossmouth**: 4.05% (3 games)

### 🏷️ Characteristic Tags

- **Singleplayer**: 90.54% (67 games)
- **Local Multiplayer**: 9.46% (7 games)
- **Mystery**: 9.46% (7 games)
- **Dystopian**: 9.46% (7 games)
- **Colorful**: 9.46% (7 games)

---

## Community 1
**Size:** 181 games

### 🎮 Top Genres

- **Early Access**: 9.94% (18 games)
- **Simulation**: 9.39% (17 games)
- **Indie**: 65.19% (118 games)
- **Action**: 62.98% (114 games)
- **Adventure**: 45.3% (82 games)

### 🏢 Top Publishers

- **Valve**: 8.29% (15 games)
- **Aspyr**: 2.76% (5 games)
- **505 Games**: 2.21% (4 games)
- **Disney**: 2.21% (4 games)
- **NeocoreGames**: 2.21% (4 games)

### 🏷️ Characteristic Tags

- **Online Co-Op**: 9.94% (18 games)
- **Colorful**: 9.94% (18 games)
- **Controller**: 9.94% (18 games)
- **Anime**: 9.94% (18 games)
- **Comedy**: 9.94% (18 games)

---

## Community 2
**Size:** 428 games

### 🎮 Top Genres

- **Action**: 56.88% (244 games)
- **Indie**: 40.79% (175 games)
- **Adventure**: 38.69% (166 games)
- **Racing**: 3.73% (16 games)
- **Free To Play**: 3.26% (14 games)

### 🏢 Top Publishers

- **THQ Nordic**: 5.13% (22 games)
- **Fulqrum Publishing**: 2.8% (12 games)
- **2K**: 2.56% (11 games)
- **Ubisoft**: 2.56% (11 games)
- **Square Enix**: 2.56% (11 games)

### 🏷️ Characteristic Tags

- **Hack and Slash**: 9.79% (42 games)
- **Dark**: 9.79% (42 games)
- **Sandbox**: 9.79% (42 games)
- **Stealth**: 9.79% (42 games)
- **Tactical**: 9.56% (41 games)

---

## Community 3
**Size:** 114 games

### 🎮 Top Genres

- **Action**: 6.09% (7 games)
- **Free To Play**: 0.87% (1 games)
- **Casual**: 0.87% (1 games)

### 🏢 Top Publishers

- **Crystal Dynamics**: 0.87% (1 games)
- **Valve**: 0.87% (1 games)
- **Milk Carton Games**: 0.87% (1 games)
- **Gunship_Mark_II**: 0.87% (1 games)
- **Llamasoft Ltd.**: 0.87% (1 games)

### 🏷️ Characteristic Tags

- **Action**: 9.57% (11 games)
- **Movie**: 6.09% (7 games)
- **Horror**: 4.35% (5 games)
- **Multiplayer**: 4.35% (5 games)
- **Adventure**: 3.48% (4 games)

---

## Community Analysis Summary

### Key Insights

- **Total Communities Analyzed**: 4
- **Community Detection Algorithm**: Louvain
- **Primary Features Analyzed**: Genres, Publishers, Tags, Platform Support, Pricing

### Methodology

1. **Community Detection**: Applied Louvain algorithm to cosine similarity graph of Steam games
2. **Feature Analysis**: Calculated percentage distributions of key game features within each community
3. **Profile Generation**: Identified top characteristics that define each community

*Report generated on: 2025-09-20 16:06:00*