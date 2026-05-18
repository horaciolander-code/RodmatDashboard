import { useState } from 'react';
import api from '../services/api';
import { useAuth } from '../context/AuthContext';

interface StepResult {
  total_rows: number;
  inserted: number;
  updated: number;
  errors: number;
  unknown_skus?: string[];
  warning?: string;
}

interface RodmatStep {
  key: string;
  label: string;
  endpoint: string;
  accept: string;
  desc: string;
  template: string;
}

interface ExternalStep {
  key: string;
  label: string;
  endpoint: string;
  accept: string;
  desc: string;
}

const RODMAT_STEPS: RodmatStep[] = [
  { key: 'products',  label: '1. Productos',          endpoint: '/import/products',         accept: '.xlsx,.xls', desc: 'Productos individualizados.xlsx',          template: 'products' },
  { key: 'combos',   label: '2. Combos',              endpoint: '/import/combos',           accept: '.xlsx,.xls', desc: 'Listado de combos tiktok.xlsx',            template: 'combos' },
  { key: 'initial',  label: '3. Inventario Inicial',  endpoint: '/import/initial-inventory', accept: '.xlsx,.xls', desc: 'Inventario inicial.xlsx',                  template: 'initial-inventory' },
  { key: 'incoming', label: '4. Stock Pendiente',     endpoint: '/import/incoming-stock',   accept: '.xlsx,.xls', desc: 'Inventario pendiente de recibir.xlsx',     template: 'incoming-stock' },
];

const EXTERNAL_STEPS: ExternalStep[] = [
  { key: 'orders',    label: '5. TikTok Orders',   endpoint: '/import/orders',     accept: '.csv',          desc: 'AllBBDD.csv (pedidos principales)' },
  { key: 'affiliates',label: '6. Afiliadas',        endpoint: '/import/affiliates', accept: '.csv',          desc: 'CSV de afiliadas (TikTok Creator Center)' },
  { key: 'amazon',   label: '7. Amazon Orders',    endpoint: '/import/amazon',     accept: '.txt,.tsv,.csv', desc: 'Amazon order report .txt (Seller Central → Reports → Order Reports → All Orders)' },
];

export default function DataImport() {
  const { isSuperadmin, isWarehouse, activeStoreId, activeStoreName } = useAuth();
  const [results, setResults] = useState<Record<string, StepResult | null>>({});
  const [loading, setLoading] = useState<string | null>(null);

  const handleUpload = async (step: RodmatStep | ExternalStep, file: File) => {
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

  const renderResult = (key: string) => {
    const result = results[key];
    if (!result) return null;

    if (result.errors === -1) {
      return <span className="text-sm text-red-500 font-medium">Error de conexión</span>;
    }

    if (result.warning) {
      return (
        <div className="text-sm text-amber-700 bg-amber-50 border border-amber-300 rounded-lg px-3 py-2 max-w-sm">
          <p className="font-semibold mb-1">Inventario inicial ya cargado</p>
          <p>{result.warning}</p>
        </div>
      );
    }

    if (result.unknown_skus && result.unknown_skus.length > 0) {
      return (
        <div className="text-sm text-red-700 bg-red-50 border border-red-200 rounded-lg px-3 py-2 max-w-sm">
          <p className="font-semibold mb-1">Bloqueado — {result.unknown_skus.length} SKU(s) no encontrados en el catálogo:</p>
          <ul className="list-disc list-inside space-y-0.5">
            {result.unknown_skus.map(sku => (
              <li key={sku} className="font-mono text-xs">{sku}</li>
            ))}
          </ul>
          <p className="mt-1 text-xs text-red-500">Carga primero el fichero de Productos o corrige los nombres.</p>
        </div>
      );
    }

    return (
      <span className="text-sm text-green-600 font-medium">
        {result.inserted} insertados, {result.updated} actualizados
        {result.errors > 0 && <span className="text-red-500"> ({result.errors} errores)</span>}
      </span>
    );
  };

  const renderUploadButton = (step: RodmatStep | ExternalStep) => {
    const isLoading = loading === step.key;
    return (
      <label className={`cursor-pointer px-4 py-2 rounded text-sm font-medium ${
        isLoading ? 'bg-gray-200 text-gray-500 cursor-wait' : 'bg-blue-600 text-white hover:bg-blue-700'
      }`}>
        {isLoading ? 'Subiendo...' : 'Subir'}
        <input
          type="file"
          accept={step.accept}
          className="hidden"
          disabled={isLoading}
          onChange={e => { const f = e.target.files?.[0]; if (f) handleUpload(step, f); e.target.value = ''; }}
        />
      </label>
    );
  };

  return (
    <div>
      <div className="flex items-center justify-between mb-2">
        <h1 className="text-2xl font-bold">Importación de Datos</h1>
        {isSuperadmin && (
          <span className="text-sm px-3 py-1 bg-blue-50 border border-blue-200 rounded-full text-blue-700">
            Importando a: <strong>{activeStoreName}</strong>
          </span>
        )}
      </div>
      <p className="text-gray-500 mb-8">
        Sube los archivos en orden. Primero los datos RODMAT (productos, combos, inventario), luego las cargas externas (pedidos).
      </p>

      {/* ── Sección 1: Datos RODMAT ── */}
      <div className="mb-8">
        <div className="flex items-center gap-3 mb-3">
          <h2 className="text-lg font-semibold text-gray-800">Datos RODMAT</h2>
          <span className="text-xs px-2 py-0.5 bg-blue-100 text-blue-700 rounded-full">Catálogo e inventario</span>
        </div>
        <p className="text-sm text-gray-400 mb-4">
          Descarga la plantilla de cada fichero para asegurarte de usar el formato correcto antes de subir.
        </p>

        <div className="space-y-3">
          {RODMAT_STEPS.filter(step => !isWarehouse || step.key === 'initial' || step.key === 'incoming').map(step => {
            const result = results[step.key];
            const hasUnknownSkus = result?.unknown_skus && result.unknown_skus.length > 0;
            return (
            <div key={step.key} className={`bg-white border rounded-lg p-4 ${hasUnknownSkus ? 'border-red-300' : ''}`}>
              <div className="flex items-center justify-between">
                <div>
                  <h3 className="font-semibold text-gray-800">{step.label}</h3>
                  <p className="text-sm text-gray-500">{step.desc}</p>
                </div>
                <div className="flex items-center gap-2">
                  {!hasUnknownSkus && renderResult(step.key)}
                  <a
                    href={`/api/import/templates/${step.template}`}
                    download
                    className="px-3 py-2 rounded text-sm font-medium border border-gray-300 text-gray-600 hover:bg-gray-50 whitespace-nowrap"
                    title="Descargar plantilla Excel"
                  >
                    Plantilla
                  </a>
                  {renderUploadButton(step)}
                </div>
              </div>
              {hasUnknownSkus && (
                <div className="mt-3">
                  {renderResult(step.key)}
                </div>
              )}
            </div>
            );
          })}
        </div>
      </div>

      {/* ── Sección 2: Cargas Externas (solo roles no-warehouse) ── */}
      {!isWarehouse && <div>
        <div className="flex items-center gap-3 mb-3">
          <h2 className="text-lg font-semibold text-gray-800">Cargas Externas</h2>
          <span className="text-xs px-2 py-0.5 bg-orange-100 text-orange-700 rounded-full">Pedidos de plataformas</span>
        </div>
        <p className="text-sm text-gray-400 mb-4">
          Exporta los informes directamente desde TikTok Shop y Amazon Seller Central y súbelos aquí.
        </p>

        <div className="space-y-3">
          {EXTERNAL_STEPS.map(step => (
            <div key={step.key} className="bg-white border rounded-lg p-4">
              <div className="flex items-center justify-between">
                <div>
                  <h3 className="font-semibold text-gray-800">{step.label}</h3>
                  <p className="text-sm text-gray-500">{step.desc}</p>
                </div>
                <div className="flex items-center gap-2">
                  {renderResult(step.key)}
                  {renderUploadButton(step)}
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>}
    </div>
  );
}
