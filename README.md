# NAV/BC Data Archiving Solution

A modular, Python-based ETL pipeline for migrating company-specific data from Microsoft Dynamics Business Central/NAV live databases to archive databases. Built for performance, resumability, and observability.

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![SQL Server](https://img.shields.io/badge/SQL%20Server-2016+-green.svg)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)

---

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Architecture](#architecture)
- [Logging & Monitoring](#logging--monitoring)
- [Error Handling](#error-handling)
- [Troubleshooting](#troubleshooting)
- [Performance Tips](#performance-tips)
- [License](#license)

---

## 📖 Overview

This solution addresses the challenge of archiving Dynamics NAV/BC data while handling the dynamic schema naming conventions (`[Company Name$Table]`). It leverages SQL Server's native performance for data movement while using Python for orchestration, version control, and observability.

**Key Design Decision:** Since both source and target databases reside on the same SQL Server instance, data movement is performed via server-side `INSERT INTO ... SELECT` statements, bypassing network latency and Python memory overhead.

---

## ✨ Features

| Feature | Description |
|---------|-------------|
| **Dynamic Schema Discovery** | Automatically detects company tables using `[Company$%]` prefix |
| **Resume Capability** | Checkpoint tracking via `ETL_State` table for crash recovery |
| **Idempotent Loads** | `NOT EXISTS` clause prevents duplicate rows on retry |
| **Keyset Pagination** | `WHERE Key > LastKey` for optimal performance on large tables |
| **Constraint Management** | Per-table constraint disable/enable for data integrity |
| **Timestamp Exclusion** | Automatically excludes `rowversion`/`timestamp` columns |
| **Progress Tracking** | Real-time progress bars with `tqdm` |
| **Detailed Logging** | File + Console logging with timestamps and context |
| **Summary Reports** | Final report with rows migrated, throughput, and errors |
| **Modular OOP Design** | Clean separation of concerns for maintainability |

---

## 🛠 Prerequisites

- **Python:** 3.8 or higher
- **SQL Server:** 2016 or higher
- **ODBC Driver:** ODBC Driver 17 for SQL Server
- **Database Access:** Read access to Live DB, Write access to Archive DB

---

## 📦 Installation

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/nav-bc-archive-migration.git
cd nav-bc-archive-migration
