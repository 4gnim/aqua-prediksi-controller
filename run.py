import requests
import sqlite3
import json
from datetime import datetime, timedelta

# --- 1. KONFIGURASI ---
API_URL_BMKG = "https://api.bmkg.go.id/publik/prakiraan-cuaca?adm4=32.78.08.1008"
NAMA_DATABASE = "aquaprediksi.db"  # Database akan ada di folder yang sama

# --- 2. FUNGSI DATABASE (SQLite) ---


def init_database():
    """Membuat database dan tabel jika belum ada."""
    print(f"Membuka database: {NAMA_DATABASE}")
    conn = sqlite3.connect(NAMA_DATABASE)
    cursor = conn.cursor()

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS PrakiraanCuaca (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        waktu_lokal DATETIME NOT NULL,
        waktu_ambil_data DATETIME NOT NULL,
        suhu_c REAL,
        kelembapan_persen REAL,
        curah_hujan_mm REAL,
        kondisi_cuaca TEXT
    );
    """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS LogKeputusanIrigasi (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        waktu_keputusan DATETIME NOT NULL,
        keputusan TEXT NOT NULL,
        alasan TEXT
    );
    """
    )

    conn.commit()
    conn.close()
    print("Database siap.\n")


def simpan_data_prakiraan(data_list):
    """Menyimpan list data prakiraan ke database."""
    conn = sqlite3.connect(NAMA_DATABASE)
    cursor = conn.cursor()
    waktu_ambil = datetime.now()

    for data in data_list:
        cursor.execute(
            """
        INSERT INTO PrakiraanCuaca (waktu_lokal, waktu_ambil_data, suhu_c, kelembapan_persen, curah_hujan_mm, kondisi_cuaca)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
            (
                data["waktu_lokal"],
                waktu_ambil,
                data["suhu"],
                data["kelembapan"],
                data["curah_hujan"],
                data["cuaca"],
            ),
        )

    conn.commit()
    conn.close()
    print(f"Berhasil menyimpan {len(data_list)} data prakiraan ke database.")


def simpan_keputusan_irigasi(keputusan, alasan):
    """Menyimpan hasil keputusan logika irigasi ke database."""
    conn = sqlite3.connect(NAMA_DATABASE)
    cursor = conn.cursor()

    cursor.execute(
        """
    INSERT INTO LogKeputusanIrigasi (waktu_keputusan, keputusan, alasan)
    VALUES (?, ?, ?)
    """,
        (datetime.now(), keputusan, alasan),
    )

    conn.commit()
    conn.close()
    print(f"Berhasil menyimpan keputusan: {keputusan} (Alasan: {alasan})")


# --- 3. FUNGSI PENGAMBIL DATA API (Sama seperti sebelumnya) ---


def ambil_data_cuaca_bmkg():
    print(f"Mengambil data dari BMKG: {API_URL_BMKG}")
    try:
        response = requests.get(API_URL_BMKG, timeout=10)
        response.raise_for_status()
        data = response.json()
        prakiraan_data_raw = data.get("data", [{}])[0].get("cuaca", [])

        prakiraan_bersih = []
        if not prakiraan_data_raw:
            print("Error: 'cuaca' key tidak ditemukan di JSON.")
            return None

        for data_per_hari in prakiraan_data_raw:
            for prakiraan_per_3_jam in data_per_hari:
                waktu_lokal = datetime.strptime(
                    prakiraan_per_3_jam.get("local_datetime"), "%Y-%m-%d %H:%M:%S"
                )
                data_point = {
                    "waktu_lokal": waktu_lokal,
                    "suhu": prakiraan_per_3_jam.get("t"),
                    "kelembapan": prakiraan_per_3_jam.get("hu"),
                    "cuaca": prakiraan_per_3_jam.get("weather_desc"),
                    "curah_hujan": prakiraan_per_3_jam.get("tp", 0.0),
                }
                prakiraan_bersih.append(data_point)

        print(f"Berhasil mengambil dan mem-parsing {len(prakiraan_bersih)} data poin.")
        return prakiraan_bersih

    except requests.exceptions.RequestException as e:
        print(f"Error saat mengambil data API: {e}")
        return None


# --- 4. FUNGSI LOGIKA KEPUTUSAN (Sama seperti sebelumnya) ---


def tentukan_keputusan_irigasi(data_prakiraan):
    if not data_prakiraan:
        return "ERROR", "Tidak ada data prakiraan untuk dianalisis."

    sekarang = datetime.now()
    batas_waktu_24jam = sekarang + timedelta(hours=24)

    prakiraan_24jam = [
        p
        for p in data_prakiraan
        if p["waktu_lokal"] > sekarang and p["waktu_lokal"] <= batas_waktu_24jam
    ]

    if not prakiraan_24jam:
        return (
            "TUNDA",
            "Tidak ada data prakiraan untuk 24 jam ke depan (data mungkin basi).",
        )

    total_curah_hujan = sum(float(p["curah_hujan"]) for p in prakiraan_24jam)
    avg_suhu = sum(float(p["suhu"]) for p in prakiraan_24jam) / len(prakiraan_24jam)
    avg_kelembapan = sum(float(p["kelembapan"]) for p in prakiraan_24jam) / len(
        prakiraan_24jam
    )

    print(f"\n--- Analisis 24 Jam ke Depan ---")
    print(f"Total Curah Hujan: {total_curah_hujan:.2f} mm")
    print(f"Rata-rata Suhu: {avg_suhu:.2f}°C")
    print(f"Rata-rata Kelembapan: {avg_kelembapan:.2f}%")
    print("---------------------------------")

    if total_curah_hujan > 5.0:
        return (
            "TUNDA",
            f"Prediksi total curah hujan {total_curah_hujan:.2f} mm dalam 24 jam.",
        )
    if total_curah_hujan < 1.0 and (avg_suhu > 30.0 or avg_kelembapan < 60.0):
        return (
            "IRIGASI_EKSTRA",
            f"Suhu tinggi ({avg_suhu:.2f}°C) dan/atau kelembapan rendah ({avg_kelembapan:.2f}%).",
        )
    return (
        "IRIGASI_NORMAL",
        f"Kondisi cuaca normal (Hujan: {total_curah_hujan:.2f}mm, Suhu: {avg_suhu:.2f}°C).",
    )


# --- 5. FUNGSI UTAMA (MAIN) ---


def jalankan_proses_irigasi_lengkap():
    print(f"\n==========================================")
    print(f"MEMULAI JOB - {datetime.now()}")
    print(f"==========================================")

    list_prakiraan = ambil_data_cuaca_bmkg()

    if list_prakiraan:
        simpan_data_prakiraan(list_prakiraan)
        (keputusan, alasan) = tentukan_keputusan_irigasi(list_prakiraan)
        simpan_keputusan_irigasi(keputusan, alasan)
    else:
        print("Gagal mengambil data cuaca. Proses dihentikan.")
        simpan_keputusan_irigasi("ERROR", "Gagal mengambil data dari API BMKG.")

    print(f"\n--- JOB SELESAI - {datetime.now()} ---")


# --- Program Entry Point ---
if __name__ == "__main__":
    init_database()
    jalankan_proses_irigasi_lengkap()
