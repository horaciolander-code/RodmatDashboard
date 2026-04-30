import { useState } from 'react';
import api from '../services/api';
import { useAuth } from '../context/AuthContext';

interface StepResult {
  total_rows: number;
  inserted: number;
  updated: number;
  errors: number;
}

const STEPS = [
  { key: 'products', label: '1. Products', endpoint: '/import/products', accept: '.xlsx,.xls', desc: 'Upload Productos individualizados.xlsx' },
  { key: 'combos', label: '2. Combos', endpoint: '/import/combos', accept: '.xlsx,.xls', desc: 'Upload Listado de combos tiktok.xlsx' },
  { key: 'initial', label: '3. Initial Inventory', endpoint: '/import/initial-inventory', accept: '.xlsx,.xls', desc: 'Upload Inventario inicial.xlsx' },
  { key: 'incoming', label: '4. Incoming Stock', endpoint: '/import/incoming-stock', accept: '.xlsx,.xls', desc: 'Upload Inventario pendiente de recibir.xlsx' },
  { key: 'orders', label: '5. Orders', endpoint: '/import/orders', accept: '.csv', desc: 'Upload AllBBDD.csv (main orders)' },
  { key: 'affiliates', label: '6. Affiliates', endpoint: '/import/affiliates', accept: '.csv', desc: 'Upload Afiliadas CSV files' },
];

export default function DataImport() {
  const { isSuperadmin, activeStoreId, activeStoreName } = useAuth();
  const [results, setResults] = useState<Record<string, StepResult | null>>({});
  const [loading, setLoading] = useState<string | null>(null);

  const handleUpload = async (step: typeof STEPS[0], file: File) => {
    setLoading(step.key);
    try {
      const form = new FormData();
      form.append('file', file);
      const url = isSuperadmin
        ? `${step.endpoint}?store_id=${activeStoreId}`
        : step.endpoint;
      const res = await api.post(url, form, {
        headers: { 'Content-Type': 'multipart/form-data' },
        timeout: 120000,
      });
      setResults(prev => ({ ...prev, [step.key]: res.data }));
    } catch {
      setResults(prev => ({ ...prev, [step.key]: { total_rows: 0, inserted: 0, updated: 0, errors: -1 } }));
    }
    setLoading(null);
  };

  return (
    <div>
      <div className="flex items-center justify-between mb-2">
        <h1 className="text-2xl font-bold">Data Import</h1>
        {isSuperadmin && (
          <span className="text-sm px-3 py-1 bg-blue-50 border border-blue-200 rounded-full text-blue-700">
            Importing to: <strong>{activeStoreName}</strong>
          </span>
        )}
      </div>
      <p className="text-gray-500 mb-6">Upload TikTok Shop files in order. Products first, then combos, inventory, and finally orders.</p>

      <div className="space-y-4">
        {STEPS.map(step => {
          const result = results[step.key];
          const isLoading = loading === step.key;

          return (
            <div key={step.key} className="bg-white border rounded-lg p-4">
              <div className="flex items-center justify-between">
                <div>
                  <h3 className="font-semibold">{step.label}</h3>
                  <p className="text-sm text-gray-500">{step.desc}</p>
                </div>
                <div className="flex items-center gap-3">
                  {result && result.errors !== -1 && (
                    <span className="text-sm text-green-600">
                      {result.inserted} inserted, {result.updated} updated
                      {result.errors > 0 && <span className="text-red-500"> ({result.errors} errors)</span>}
                    </span>
                  )}
                  {result && result.errors === -1 && (
                    <span className="text-sm text-red-500">Upload failed</span>
                  )}
                  <label className={`cursor-pointer px-4 py-2 rounded text-sm ${
                    isLoading ? 'bg-gray-300 cursor-wait' : 'bg-blue-600 text-white hover:bg-blue-700'
                  }`}>
                    {isLoading ? 'Uploading...' : 'Upload'}
                    <input type="file" accept={step.accept} className="hidden"
                      disabled={isLoading}
                      onChange={e => { const f = e.target.files?.[0]; if (f) handleUpload(step, f); e.target.value = ''; }} />
                  </label>
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
