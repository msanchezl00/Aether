export async function GET() {
  try {
    const res = await fetch("http://ranking:8000/process_hdfs_avro_data", {
      method: "GET",
    });

    if (!res.ok) {
      return new Response(JSON.stringify({ error: "Fallo en backend" }), { status: res.status });
    }

    const res2 = await fetch("http://ranking:8000/create_invert_index", {
      method: "POST",
    });

    if (!res2.ok) {
      return new Response(JSON.stringify({ error: "Fallo en backend" }), { status: res2.status });
    }

    return new Response(JSON.stringify({ success: true }), {
      headers: { "Content-Type": "application/json" },
    });
  } catch (err) {
    console.error(err);
    return new Response(JSON.stringify({ error: "Conexión fallida" }), { status: 500 });
  }
}