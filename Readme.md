**1. Inisialisasi Lingkungan dan Library**
--
Langkah:

Menginstal library: pyspark untuk analisis data besar dan kaleido untuk menyimpan visualisasi Plotly.
Membuat Spark Session: Sebagai entry point untuk menggunakan API PySpark.
Tujuan:

Menyiapkan lingkungan yang diperlukan untuk pemrosesan dan visualisasi data.
    '''
    from pyspark.sql import SparkSession
    spark_session = SparkSession.builder.appName('customer_analysis').master('local').getOrCreate()
    '''

**2. Membaca dan Membersihkan Dataset**
--
Langkah:

Dataset calendar.csv, customer_flight_activity.csv, dan customer_loyalty_history.csv dibaca menggunakan PySpark.
Kolom _c0 yang tidak relevan dihapus.
Skema data didefinisikan secara eksplisit untuk memastikan tipe data yang sesuai.

Tujuan:

Memuat data mentah ke dalam Spark DataFrame.
Melakukan pembersihan awal untuk mempersiapkan analisis.
    '''
        from pyspark.sql.types import *
        schema = StructType([
            StructField("_c0", StringType(), True),
            StructField("lovalty_no", StringType(), True),
            StructField("year", StringType(), True),
            StructField("month", StringType(), True),
            ...
        ])
        df_customer_flight_activity = spark_session.read.csv('/content/customer_flight_activity.csv', header=True, schema=schema)
        df_customer_flight_activity = df_customer_flight_activity.drop("_c0")
    '''

**3. Transformasi Dataset**
--
Langkah:

Pada dataset calendar.csv:
Ditambahkan kolom seperti start_of_the_year, start_of_the_quarter, dan start_of_the_month.
Pada dataset aktivitas penerbangan:
Data diaggresikan berdasarkan tahun dan bulan.
Pada dataset loyalitas pelanggan:
Data dianalisis berdasarkan atribut demografis seperti pendidikan dan tingkat loyalitas.

Tujuan: Memperkaya dataset dengan informasi baru untuk analisis yang lebih mendalam.
   
    '''
        aggregated_calendar_df = calendar_df.withColumn("start_of_the_year", year(col("date"))) \
                                            .withColumn("start_of_the_quarter", quarter(col("date"))) \
                                            .withColumn("start_of_the_month", month(col("date")))
    '''
    
**4. Visualisasi Data**
--
Langkah:

Dataset yang telah diolah dikonversi ke Pandas untuk visualisasi menggunakan Plotly.
Grafik yang dibuat:
Bar Chart: Distribusi aktivitas berdasarkan tahun dan bulan.
Scatter Plot: Hubungan jarak penerbangan dengan poin yang diperoleh.
Pie Chart: Distribusi tingkat loyalitas pelanggan.
Bar Chart: Rata-rata gaji berdasarkan tingkat pendidikan.

Tujuan:

Memberikan insight visual yang mudah dipahami dari data.
    '''
        fig1 = px.bar(calendar_agg, x="Month", y="count", color="Year",
                    title="Distribution of Events Over Time",
                    labels={"count": "Event Count", "Month": "Month"})
        fig1.show()
    '''

**5. Analisis dan Penyimpanan Visualisasi**
--
Langkah:

Visualisasi disimpan ke file .png untuk dokumentasi lebih lanjut.
File visualisasi:
distribution of events over time.png
distance vs. points accumulated.png
customer loyalty levels distribution.png

Tujuan:

Menyimpan hasil analisis sehingga dapat digunakan dalam laporan atau presentasi.

**Analisi Grafik**
--
1. Rata-Rata Gaji Berdasarkan Tingkat Pendidikan

Observasi:

Individu dengan gelar Doktor memiliki rata-rata gaji tertinggi, jauh lebih tinggi dibandingkan dengan tingkat pendidikan lainnya.
Pemegang gelar Magister memiliki gaji rata-rata yang lebih tinggi dibandingkan dengan pemegang gelar Sarjana.
Individu dengan tingkat pendidikan SMA atau lebih rendah memiliki gaji rata-rata terendah.

Wawasan:

Terdapat hubungan positif yang jelas antara tingkat pendidikan yang lebih tinggi dan rata-rata gaji.
Organisasi yang menargetkan pelanggan bernilai tinggi dapat fokus pada individu dengan tingkat pendidikan yang lebih tinggi untuk program loyalitas atau layanan premium.

2. Jarak Tempuh vs. Poin yang Dikumpulkan

Observasi:

Grafik scatter menunjukkan hubungan linear yang kuat antara jarak penerbangan dan poin yang dikumpulkan.
Semakin jauh jarak yang ditempuh, semakin tinggi poin yang dikumpulkan.

Wawasan:

Sistem akumulasi poin dalam program loyalitas langsung terkait dengan jarak penerbangan, yang mencerminkan sistem penghargaan yang adil dan dapat diprediksi.
Mendorong pelanggan untuk melakukan penerbangan jarak jauh atau lebih sering dapat menjadi strategi yang efektif untuk meningkatkan keterlibatan dalam program loyalitas.

3. Distribusi Kegiatan Sepanjang Waktu

Observasi:

Kegiatan terdistribusi secara konsisten sepanjang bulan dalam setahun, tanpa adanya variasi musiman yang signifikan.
Distribusi kegiatan juga merata sepanjang tahun, meskipun beberapa tahun tertentu (misalnya, 2016 dan 2017) mungkin menunjukkan sedikit lebih banyak aktivitas.

Wawasan:

Aktivitas organisasi terdistribusi dengan baik sepanjang tahun, menunjukkan keterlibatan yang konsisten atau proses operasional yang stabil.
Informasi ini dapat digunakan untuk perencanaan sumber daya dan penjadwalan acara promosi guna mempertahankan atau meningkatkan keterlibatan pelanggan selama bulan-bulan dengan aktivitas yang lebih rendah.
