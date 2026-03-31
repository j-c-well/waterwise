import { kv } from '@vercel/kv';

export default async function handler(req, res) {
  try {
    const data = await kv.get('waterwise:latest');

    if (!data) {
      return res.status(404).json({ error: 'No data yet — scrape has not run' });
    }

    res.setHeader('Cache-Control', 's-maxage=300, stale-while-revalidate');
    return res.status(200).json(data);
  } catch (err) {
    console.error('Data fetch error:', err);
    return res.status(500).json({ error: err.message });
  }
}
