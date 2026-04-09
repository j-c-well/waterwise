const Anthropic = require('@anthropic-ai/sdk');

const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

const CORS = (res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
};

module.exports = async function handler(req, res) {
  CORS(res);
  if (req.method === 'OPTIONS') { res.status(200).end(); return; }
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const { image, mimeType = 'image/jpeg' } = req.body || {};
  if (!image) return res.status(400).json({ error: 'Missing image field' });

  try {
    const message = await client.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 256,
      messages: [
        {
          role: 'user',
          content: [
            {
              type: 'image',
              source: { type: 'base64', media_type: mimeType, data: image },
            },
            {
              type: 'text',
              text: `This is a photo of a home appliance label or nameplate. Extract the brand name and model number.

Respond with JSON only, no explanation:
{
  "brand": "<brand name or null>",
  "model": "<model number or null>",
  "applianceType": "<one of: dishwasher, washingMachine, dryer, refrigerator, toilet, shower, faucet, other, or null>",
  "confidence": "<high if both brand and model clearly visible, low otherwise>",
  "rawText": "<any other relevant text from the label>"
}`,
            },
          ],
        },
      ],
    });

    const text = message.content[0]?.text?.trim() ?? '';

    // Parse JSON from response (Claude may wrap in backticks)
    const jsonMatch = text.match(/\{[\s\S]*\}/);
    if (!jsonMatch) {
      return res.status(200).json({ confidence: 'low', brand: null, model: null, rawText: text });
    }

    const result = JSON.parse(jsonMatch[0]);
    return res.status(200).json(result);
  } catch (err) {
    console.error('scan-label error:', err.message);
    return res.status(500).json({ error: err.message });
  }
};
