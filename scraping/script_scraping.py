"""
Script : ffhandball_nord_scraper.py
Pré-requis :
pip install requests beautifulsoup4 pdfplumber pandas sqlalchemy tabulate
(tabu la optional)
"""
import re
import os
import time
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
import pdfplumber
import pandas as pd
from sqlalchemy import create_engine

# --- CONFIG ---
BASE_POULE_URL = "https://www.ffhandball.fr/competitions/saison-2025-2026-21/departemental/c59-1ere-division-nord-mas-28289/poule-168745/"
OUTPUT_DIR = "ffh_nord_data"
PDF_DIR = os.path.join(OUTPUT_DIR, "pdfs")
DB_PATH = os.path.join(OUTPUT_DIR, "ffh_nord.sqlite")
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; ffh-scraper/1.0)"}
os.makedirs(PDF_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- HELPERS ---
def get_soup(url):
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def find_pdf_links(soup, base_url):
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".pdf"):
            links.append(urljoin(base_url, href))
    return list(dict.fromkeys(links))  # unique

def download_file(url, out_folder):
    filename = os.path.basename(urlparse(url).path)
    out_path = os.path.join(out_folder, filename)
    if os.path.exists(out_path):
        return out_path
    r = requests.get(url, headers=HEADERS, stream=True, timeout=20)
    r.raise_for_status()
    with open(out_path, "wb") as f:
        for chunk in r.iter_content(1024*8):
            if chunk:
                f.write(chunk)
    return out_path

# Very simple table extraction from FDME-like PDFs:
def extract_stats_from_pdf(pdf_path):
    """
    Retourne dict {match_id, team1_name, team2_name, goals: [{player, goals, number}], ...}
    Approche heuristique : recherche de tables contenant "Nom", "But(s)", "Equipe" ou colonnes similaires.
    """
    results = {"pdf": pdf_path, "players": []}
    with pdfplumber.open(pdf_path) as pdf:
        text_first_pages = ""
        for i, page in enumerate(pdf.pages[:6]):  # pages de tête
            text_first_pages += "\n" + page.extract_text() or ""
            # try to extract tables on each page
            try:
                for table in page.extract_tables():
                    # table is list of rows
                    # normalize header row
                    header = [ (c or "").strip().lower() for c in (table[0] if table else []) ]
                    # find likely columns: nom/prenom, n°, buts, ...
                    if any("nom" in h or "joueur" in h or "numero" in h or "but" in h for h in header):
                        # process rows
                        for row in table[1:]:
                            rowd = dict(zip(header, row))
                            # heuristique: find name and goals
                            # possible header names: 'nom', 'prénom', 'n°', 'but(s)', 'Buts'
                            name = None
                            goals = None
                            for colk, colv in rowd.items():
                                if colk and ("nom" in colk or "joueur" in colk):
                                    name = (colv or "").strip()
                                if colk and ("but" in colk):
                                    goals = (colv or "").strip()
                            if name and goals:
                                # try int conversion
                                try:
                                    g = int(re.findall(r"\d+", goals)[0])
                                except Exception:
                                    try:
                                        g = int(goals)
                                    except:
                                        g = None
                                results["players"].append({"name": name, "goals": g})
            except Exception:
                pass
    results["raw_text_head"] = text_first_pages[:5000]
    return results

# --- MAIN FLOW ---
def scrape_poule(poule_url):
    soup = get_soup(poule_url)
    # 1) find match pages (links with '/match' or 'rencontre' OR direct pdfs)
    match_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/match" in href or "rencontre" in href or "feuille" in href or href.lower().endswith(".pdf"):
            match_links.append(urljoin(poule_url, href))
    match_links = list(dict.fromkeys(match_links))
    print(f"Found {len(match_links)} candidate links (match/pdf).")
    all_extracted = []
    for link in match_links:
        try:
            print("Processing:", link)
            if link.lower().endswith(".pdf"):
                pdf_path = download_file(link, PDF_DIR)
                info = extract_stats_from_pdf(pdf_path)
                info["source_url"] = link
                all_extracted.append(info)
            else:
                # open link and see if there's PDF inside
                s2 = get_soup(link)
                pdfs = find_pdf_links(s2, link)
                if pdfs:
                    for p in pdfs:
                        pdf_path = download_file(p, PDF_DIR)
                        info = extract_stats_from_pdf(pdf_path)
                        info["source_url"] = link
                        all_extracted.append(info)
                else:
                    # sometimes the match page contains the score table in HTML
                    # attempt to parse goals from html tables/text
                    tables = s2.find_all("table")
                    found = False
                    for t in tables:
                        text = t.get_text(separator="|").lower()
                        if "but" in text or "buts" in text or "nom" in text:
                            # save raw html table text as fallback
                            all_extracted.append({"source_url": link, "html_table_text": text})
                            found = True
                            break
                    if not found:
                        # keep page text head
                        all_extracted.append({"source_url": link, "html_text_head": s2.get_text()[:1000]})
            time.sleep(0.5)
        except Exception as e:
            print("Error on", link, e)
    return all_extracted

if __name__ == "__main__":
    extracted = scrape_poule(BASE_POULE_URL)
    # save results CSV + sqlite
    rows = []
    for e in extracted:
        if e.get("players"):
            for p in e["players"]:
                rows.append({
                    "pdf": os.path.basename(e["pdf"]),
                    "source_url": e.get("source_url"),
                    "player_name": p.get("name"),
                    "goals": p.get("goals"),
                })
        else:
            rows.append({"pdf": None, "source_url": e.get("source_url"), "player_name": None, "goals": None, "raw_html": e.get("html_table_text") or e.get("html_text_head") or e.get("raw_text_head")})
    df = pd.DataFrame(rows)
    csv_path = os.path.join(OUTPUT_DIR, "extracted_goals.csv")
    df.to_csv(csv_path, index=False)
    print("Saved CSV:", csv_path)
    # save to sqlite
    engine = create_engine(f"sqlite:///{DB_PATH}")
    df.to_sql("match_goals", engine, if_exists="replace", index=False)
    print("Saved to sqlite:", DB_PATH)
