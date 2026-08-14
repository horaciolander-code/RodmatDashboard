import { useState, useEffect } from 'react';
import api from '../services/api';
import { useAuth } from '../context/AuthContext';

interface KPIs {
  revenue: number; tt_cost: number; margin_tt: number;
  cogs_real: number; net_margin_real: number; net_margin_pct: number;
  settled: number; pending: number; settlement_pct: number;
  fees_total: number; shipping: number; affiliate: number;
  orders: number; lines: number; statements: number;
}
interface WeekData { week: string; revenue: number; settled: number; pending: number; margin_tt: number; orders: number; }
interface Fees { referral: number; smart_promo: number; managed: number; shipping: number; affiliate: number; seller_discount: number; }
interface TopProduct { product_name: string; revenue: number; margin: number; units: number; lines: number; }
interface Statement { statement_id: string; payout_id: string; total_income: number; total_margin: number; total_orders: number; period_start: string; period_end: string; settled_date: string; }

const fmt = (v: number) => new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(v);
const pct = (v: number) => `${v.toFixed(1)}%`;

export default function TikTokStatements() {
  const { isSuperadmin, activeStoreId } = useAuth();
  const [kpis, setKpis] = useState<KPIs | null>(null);
  const [weekly, setWeekly] = useState<WeekData[]>([]);
  const [fees, setFees] = useState<Fees | null>(null);
  const [top, setTop] = useState<TopProduct[]>([]);
  const [statements, setStatements] = useState<Statement[]>([]);
  const [brandSlug, setBrandSlug] = useState<string>('');
  const [uploading, setUploading] = useState(false);
  const [uploadMsg, setUploadMsg] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = async () => {
    setLoading(true); setError(null);
    try {
      const suffix = brandSlug ? `?brand_slug=${brandSlug}` : '';
      const store = isSuperadmin && activeStoreId ? `${suffix ? '&' : '?'}store_id=${activeStoreId}` : '';
      const q = `${suffix}${store}`;
      const [k, w, f, t, s] = await Promise.all([
        api.get(`/tiktok-statements/kpis${q}`),
        api.get(`/tiktok-statements/weekly${q}`),
        api.get(`/tiktok-statements/fees-breakdown${q}`),
        api.get(`/tiktok-statements/top-products${q}`),
        api.get(`/tiktok-statements/statements${q}`),
      ]);
      setKpis(k.data); setWeekly(w.data); setFees(f.data); setTop(t.data); setStatements(s.data);
    } catch (e: any) {
      setError(e?.response?.data?.detail || e.message);
    } finally { setLoading(false); }
  };
  useEffect(() => { load(); /* eslint-disable-next-line */ }, [brandSlug, activeStoreId]);

  const handleUpload = async (file: File) => {
    setUploading(true); setUploadMsg(null);
    try {
      const form = new FormData();
      form.append('file', file);
      const url = isSuperadmin && activeStoreId ? `/import/tiktok-statement?store_id=${activeStoreId}` : '/import/tiktok-statement';
      const res = await api.post(url, form, { headers: { 'Content-Type': 'multipart/form-data' } });
      setUploadMsg(`✓ ${res.data.message || res.data.inserted + ' líneas procesadas'}`);
      await load();
    } catch (e: any) {
      setUploadMsg(`✗ ${e?.response?.data?.detail || e.message}`);
    } finally { setUploading(false); }
  };

  // Colores paleta futurista
  const C = { bg:'#0a0e27', card:'#0f142f', card2:'#141a3d', border:'rgba(123,97,255,0.15)',
              cyan:'#00D4FF', green:'#00FF88', orange:'#FF9F45', red:'#FF6B35', crimson:'#FF3D6B',
              purple:'#7B61FF', textLight:'#e4e9ff', textDim:'#8892b0' };

  const maxWeekly = Math.max(1, ...weekly.map(w => w.revenue));

  return (
    <div style={{ minHeight:'100vh', background:C.bg, color:C.textLight, padding:'24px', fontFamily:'-apple-system, "Segoe UI", system-ui, sans-serif' }}>
      {/* Header */}
      <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:24 }}>
        <div>
          <h1 style={{ fontSize:24, fontWeight:700, background:'linear-gradient(90deg,#00D4FF,#7B61FF)', WebkitBackgroundClip:'text', WebkitTextFillColor:'transparent', margin:0 }}>
            ⚡ Finance → TikTok Statements
          </h1>
          <div style={{ color:C.textDim, fontSize:13, marginTop:4 }}>Ventas facturadas vs cobradas al banco</div>
        </div>
        <div style={{ display:'flex', gap:8 }}>
          {[{v:'',l:'Todas'},{v:'avon',l:'Avon'},{v:'luxperfumes',l:'LuxPerfumes'}].map(b => (
            <button key={b.v} onClick={() => setBrandSlug(b.v)}
              style={{ background: brandSlug===b.v ? 'rgba(0,212,255,0.2)' : 'rgba(0,212,255,0.06)',
                       border:`1px solid ${brandSlug===b.v ? C.cyan : 'rgba(0,212,255,0.2)'}`, color:C.cyan, padding:'8px 16px',
                       borderRadius:20, fontSize:12, fontWeight:600, cursor:'pointer',
                       boxShadow: brandSlug===b.v ? `0 0 12px rgba(0,212,255,0.4)` : 'none' }}>{b.l}</button>
          ))}
        </div>
      </div>

      {/* Upload button */}
      <div style={{ marginBottom:20, background:`linear-gradient(135deg,${C.card},${C.card2})`, border:`1px solid ${C.border}`, borderRadius:12, padding:16, display:'flex', alignItems:'center', gap:12 }}>
        <div style={{ flex:1 }}>
          <div style={{ fontSize:13, fontWeight:600, color:C.cyan }}>📥 Cargar Merchant Statement</div>
          <div style={{ fontSize:11, color:C.textDim, marginTop:2 }}>TikTok Seller Center → Finance → Merchant Statement → Export XLSX. Dedup automático por order_id+sku_id.</div>
        </div>
        <label style={{ cursor: uploading?'wait':'pointer', background:C.cyan, color:C.bg, padding:'10px 20px', borderRadius:8, fontSize:13, fontWeight:700, opacity: uploading?0.6:1 }}>
          {uploading ? 'Cargando...' : 'Subir XLSX'}
          <input type="file" accept=".xlsx" style={{ display:'none' }} onChange={e => e.target.files?.[0] && handleUpload(e.target.files[0])} disabled={uploading} />
        </label>
      </div>
      {uploadMsg && <div style={{ marginBottom:16, padding:12, borderRadius:8, background: uploadMsg.startsWith('✓') ? 'rgba(0,255,136,0.1)' : 'rgba(255,107,53,0.1)', border:`1px solid ${uploadMsg.startsWith('✓') ? C.green : C.red}`, color: uploadMsg.startsWith('✓') ? C.green : C.red, fontSize:13 }}>{uploadMsg}</div>}

      {loading && <div style={{ textAlign:'center', color:C.textDim, padding:40 }}>Cargando datos...</div>}
      {error && <div style={{ padding:16, borderRadius:8, background:'rgba(255,107,53,0.1)', border:`1px solid ${C.red}`, color:C.red }}>Error: {error}</div>}

      {kpis && !loading && (
      <>
      {/* KPI cards */}
      <div style={{ display:'grid', gridTemplateColumns:'repeat(4, 1fr)', gap:12, marginBottom:16 }}>
        {[
          {l:'Total Facturado', v:fmt(kpis.revenue), sub:`${kpis.orders} órdenes · ${kpis.statements} statements`, color:C.cyan},
          {l:'Cobrado (settled)', v:fmt(kpis.settled), sub:`${pct(kpis.settlement_pct)} del facturado`, color:C.green},
          {l:'Pending Payment', v:fmt(kpis.pending), sub:'pendiente banco', color:C.orange},
          {l:'Margen NETO REAL', v:fmt(kpis.net_margin_real), sub:`${pct(kpis.net_margin_pct)} sobre revenue`, color:C.purple},
        ].map((k,i) => (
          <div key={i} style={{ background:`linear-gradient(135deg,${C.card},${C.card2})`, border:`1px solid ${C.border}`, borderRadius:12, padding:16, position:'relative', overflow:'hidden' }}>
            <div style={{ position:'absolute', top:0, left:0, right:0, height:2, background:`linear-gradient(90deg,transparent,${k.color},transparent)`, opacity:0.7 }}></div>
            <div style={{ color:C.textDim, fontSize:11, textTransform:'uppercase', letterSpacing:0.8, marginBottom:6 }}>{k.l}</div>
            <div style={{ fontSize:26, fontWeight:700, color:'#fff', lineHeight:1.1 }}>{k.v}</div>
            <div style={{ fontSize:11, color:C.textDim, marginTop:6 }}>{k.sub}</div>
          </div>
        ))}
      </div>

      {/* Waterfall + gauge */}
      <div style={{ display:'grid', gridTemplateColumns:'2fr 1fr', gap:12, marginBottom:16 }}>
        {/* Waterfall */}
        <div style={{ background:`linear-gradient(135deg,${C.card},${C.card2})`, border:`1px solid ${C.border}`, borderRadius:12, padding:16 }}>
          <div style={{ fontSize:13, textTransform:'uppercase', letterSpacing:1, color:C.textDim, marginBottom:14, fontWeight:600 }}>🌊 Cascada Income → Margen NETO REAL</div>
          <div style={{ padding:'0 12px' }}>
            {(() => {
              const rev = kpis.revenue;
              const bars = [
                { l:'Income', v: rev, color: C.green, positive:true },
                { l:'Fees TT', v: kpis.fees_total, color: C.red, positive:false },
                { l:'Affiliate', v: kpis.affiliate, color: C.red, positive:false },
                { l:'COGS real', v: kpis.cogs_real, color: C.crimson, positive:false, highlight:true },
                { l:'Margen NETO', v: kpis.net_margin_real, color: C.green, positive:true, final:true },
              ];
              const maxV = Math.max(...bars.map(b => Math.abs(b.v)));
              return (
                <div style={{ display:'flex', alignItems:'flex-end', gap:14, height:180 }}>
                  {bars.map((b, i) => (
                    <div key={i} style={{ flex:1, textAlign:'center' }}>
                      <div style={{ fontSize:12, fontWeight:700, color:b.positive?C.green:C.red, marginBottom:6 }}>
                        {b.positive?'':'-'}{fmt(Math.abs(b.v))}
                      </div>
                      <div style={{ height: Math.max(20, (Math.abs(b.v)/maxV)*140), background:`linear-gradient(180deg,${b.color},${b.color}88)`,
                                    boxShadow: b.final?`0 0 16px ${b.color}`:`0 0 6px ${b.color}66`, borderRadius:4 }}></div>
                      <div style={{ fontSize:11, color:C.textDim, marginTop:6 }}>{b.l}{b.highlight?' 🆕':''}</div>
                    </div>
                  ))}
                  <div style={{ minWidth:100, textAlign:'center', paddingLeft:12, borderLeft:`1px solid ${C.border}` }}>
                    <div style={{ fontSize:34, color:C.green, fontWeight:700, filter:`drop-shadow(0 0 6px ${C.green})` }}>{pct(kpis.net_margin_pct)}</div>
                    <div style={{ fontSize:11, color:C.textDim }}>margen s/revenue</div>
                  </div>
                </div>
              );
            })()}
          </div>
        </div>

        {/* Gauge settlement */}
        <div style={{ background:`linear-gradient(135deg,${C.card},${C.card2})`, border:`1px solid ${C.border}`, borderRadius:12, padding:16 }}>
          <div style={{ fontSize:13, textTransform:'uppercase', letterSpacing:1, color:C.textDim, marginBottom:14, fontWeight:600 }}>💡 Ratio settlement</div>
          <div style={{ position:'relative', width:120, height:120, margin:'0 auto' }}>
            <svg viewBox="0 0 100 100" width="120" height="120">
              <circle cx="50" cy="50" r="40" fill="none" stroke="rgba(123,97,255,0.15)" strokeWidth="8"/>
              <circle cx="50" cy="50" r="40" fill="none" stroke={C.cyan} strokeWidth="8" strokeLinecap="round"
                strokeDasharray={`${(kpis.settlement_pct/100)*251} 251`} transform="rotate(-90 50 50)"
                style={{ filter:`drop-shadow(0 0 6px ${C.cyan})` }}/>
            </svg>
            <div style={{ position:'absolute', top:'50%', left:'50%', transform:'translate(-50%,-50%)', fontSize:24, fontWeight:700, color:C.cyan }}>{pct(kpis.settlement_pct)}</div>
          </div>
          <div style={{ textAlign:'center', fontSize:11, color:C.textDim, marginTop:8 }}>del facturado ya en banco</div>
        </div>
      </div>

      {/* Weekly bars + Top + Fees */}
      <div style={{ display:'grid', gridTemplateColumns:'2fr 1fr', gap:12, marginBottom:16 }}>
        <div style={{ background:`linear-gradient(135deg,${C.card},${C.card2})`, border:`1px solid ${C.border}`, borderRadius:12, padding:16 }}>
          <div style={{ fontSize:13, textTransform:'uppercase', letterSpacing:1, color:C.textDim, marginBottom:14, fontWeight:600 }}>📊 Semana × semana — Cobrado vs Pending</div>
          <div style={{ display:'flex', alignItems:'flex-end', gap:10, height:180 }}>
            {weekly.map((w, i) => {
              const sh = (w.settled / maxWeekly) * 160;
              const ph = (w.pending / maxWeekly) * 160;
              return (
                <div key={i} style={{ flex:1, display:'flex', flexDirection:'column', alignItems:'center', gap:4 }}>
                  <div style={{ fontSize:10, color:C.cyan, fontWeight:700 }}>{fmt(w.revenue)}</div>
                  <div style={{ width:'100%', maxWidth:44, display:'flex', flexDirection:'column', justifyContent:'flex-end', height:160 }}>
                    <div style={{ height:ph, background:`linear-gradient(180deg,${C.orange},#E65100)`, borderRadius:'4px 4px 0 0', boxShadow:`0 0 10px ${C.orange}44` }}></div>
                    <div style={{ height:sh, background:`linear-gradient(180deg,${C.cyan},#0088CC)`, borderRadius: ph>0 ? '0' : '4px 4px 0 0', boxShadow:`0 0 12px ${C.cyan}55` }}></div>
                  </div>
                  <div style={{ fontSize:10, color:C.textDim }}>{w.week.slice(5)}</div>
                </div>
              );
            })}
          </div>
        </div>

        <div style={{ background:`linear-gradient(135deg,${C.card},${C.card2})`, border:`1px solid ${C.border}`, borderRadius:12, padding:16 }}>
          <div style={{ fontSize:13, textTransform:'uppercase', letterSpacing:1, color:C.textDim, marginBottom:14, fontWeight:600 }}>🏆 Top 5 productos</div>
          {top.slice(0,5).map((p, i) => (
            <div key={i} style={{ display:'flex', alignItems:'center', gap:10, padding:'8px 0', borderBottom: i<4?`1px solid rgba(123,97,255,0.08)`:'none' }}>
              <div style={{ fontSize:12, fontWeight:700, color:C.purple, background:'rgba(123,97,255,0.1)', width:22, height:22, borderRadius:6, display:'flex', alignItems:'center', justifyContent:'center' }}>{i+1}</div>
              <div style={{ flex:1, fontSize:11, color:C.textLight, lineHeight:1.3 }}>{p.product_name?.slice(0, 42)}</div>
              <div style={{ color:C.green, fontFamily:'"SF Mono", Menlo, monospace', fontWeight:600, fontSize:11 }}>+{fmt(p.margin)}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Fees donut */}
      {fees && (
        <div style={{ background:`linear-gradient(135deg,${C.card},${C.card2})`, border:`1px solid ${C.border}`, borderRadius:12, padding:16, marginBottom:16 }}>
          <div style={{ fontSize:13, textTransform:'uppercase', letterSpacing:1, color:C.textDim, marginBottom:14, fontWeight:600 }}>💸 Desglose de fees TikTok</div>
          <div style={{ display:'flex', gap:24, alignItems:'center' }}>
            {(() => {
              const items = [
                { l:'Shipping (FBT+TT)', v: fees.shipping, c: C.cyan },
                { l:'Referral fee', v: fees.referral, c: C.purple },
                { l:'Affiliate', v: fees.affiliate, c: C.orange },
                { l:'Smart Promo', v: fees.smart_promo, c: C.green },
                { l:'Managed service', v: fees.managed, c: C.crimson },
              ];
              const total = items.reduce((s,it)=>s+it.v, 0);
              let offset = 0; const C_LEN = 220;
              return (<>
                <svg viewBox="0 0 100 100" width="140" height="140">
                  <circle cx="50" cy="50" r="35" fill="none" stroke={C.card2} strokeWidth="18"/>
                  {items.map((it, i) => {
                    const seg = total ? (it.v/total)*C_LEN : 0;
                    const el = <circle key={i} cx="50" cy="50" r="35" fill="none" stroke={it.c} strokeWidth="18"
                      strokeDasharray={`${seg} ${C_LEN}`} strokeDashoffset={-offset} transform="rotate(-90 50 50)"
                      style={{ filter: `drop-shadow(0 0 4px ${it.c}88)` }}/>;
                    offset += seg;
                    return el;
                  })}
                  <text x="50" y="52" textAnchor="middle" fill="#fff" fontSize="10" fontWeight="700">{fmt(total)}</text>
                </svg>
                <div style={{ flex:1 }}>
                  {items.map((it, i) => (
                    <div key={i} style={{ display:'flex', alignItems:'center', gap:8, margin:'6px 0', color:C.textLight, fontSize:12 }}>
                      <span style={{ width:10, height:10, borderRadius:2, background:it.c, display:'inline-block' }}></span>
                      {it.l}
                      <span style={{ marginLeft:'auto', color:'#fff', fontWeight:600, fontFamily:'"SF Mono", monospace' }}>{fmt(it.v)}</span>
                    </div>
                  ))}
                </div>
              </>);
            })()}
          </div>
        </div>
      )}

      {/* Statements grid */}
      <div style={{ background:`linear-gradient(135deg,${C.card},${C.card2})`, border:`1px solid ${C.border}`, borderRadius:12, padding:16 }}>
        <div style={{ fontSize:13, textTransform:'uppercase', letterSpacing:1, color:C.textDim, marginBottom:14, fontWeight:600 }}>💰 {statements.length} statements / payouts al banco</div>
        <div style={{ display:'grid', gridTemplateColumns:'repeat(6, 1fr)', gap:8 }}>
          {statements.slice(0, 18).map((s, i) => (
            <div key={i} style={{ background: s.settled_date ? 'rgba(0,212,255,0.08)':'rgba(255,159,69,0.08)',
                                  border:`1px solid ${s.settled_date ? 'rgba(0,212,255,0.25)':'rgba(255,159,69,0.35)'}`,
                                  padding:10, borderRadius:6, textAlign:'center', cursor:'pointer' }}>
              <div style={{ fontSize:10, color:C.textDim }}>{s.settled_date || s.period_end || '—'}</div>
              <div style={{ fontSize:14, fontWeight:700, color: s.settled_date ? C.cyan : C.orange, marginTop:2 }}>{fmt(s.total_income)}{!s.settled_date?' ⏳':''}</div>
              <div style={{ fontSize:9, color:C.textDim, marginTop:2 }}>{s.total_orders} ord.</div>
            </div>
          ))}
        </div>
      </div>
      </>)}
    </div>
  );
}
