export async function GET() {
  try {
    const res = await fetch("http://ranking:8000/load_hdfs_parquet_data", {
      method: "GET",
    });

    if (!res.ok) {
      return new Response(JSON.stringify({ error: "Fallo en backend" }), { status: res.status });
    }

    return new Response(JSON.stringify({ success: true }), {
      headers: { "Content-Type": "application/json" },
    });
  } catch (err) {
    console.error(err);
    return new Response(JSON.stringify({ error: "Conexión fallida" }), { status: 500 });
  }
}