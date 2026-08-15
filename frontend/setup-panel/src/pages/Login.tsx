import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';

export default function Login() {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const { login } = useAuth();
  const navigate = useNavigate();

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    const role = await login(email, password);
    if (role) navigate(role === 'warehouse' ? '/inventory' : '/');
    else setError('Email o contraseña inválidos');
  };

  return (
    <div className="min-h-screen flex items-center justify-center px-4"
         style={{background: 'radial-gradient(ellipse at center, #141a3d 0%, #0a0e27 70%)'}}>
      <div className="max-w-md w-full rdmt-card p-8">
        <div className="text-center mb-8">
          <h1 className="text-3xl rdmt-title tracking-wider mb-1">PANEL RODMAT</h1>
          <p className="text-xs uppercase tracking-widest" style={{color: 'var(--rdmt-text-mut)'}}>
            Multi-tenant · Multi-brand
          </p>
        </div>
        {error && (
          <div className="mb-5 p-3 rounded-lg text-sm font-medium"
               style={{
                 background: 'rgba(255,61,107,0.1)',
                 border: '1px solid rgba(255,61,107,0.3)',
                 color: 'var(--rdmt-crimson)'
               }}>
            {error}
          </div>
        )}
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-xs uppercase tracking-wider mb-1.5"
                   style={{color: 'var(--rdmt-text-mut)'}}>Email</label>
            <input type="email" placeholder="tucorreo@ejemplo.com" value={email}
              onChange={(e) => setEmail(e.target.value)} required
              className="w-full px-4 py-2.5 rounded-lg border text-base" />
          </div>
          <div>
            <label className="block text-xs uppercase tracking-wider mb-1.5"
                   style={{color: 'var(--rdmt-text-mut)'}}>Contraseña</label>
            <input type="password" placeholder="••••••••" value={password}
              onChange={(e) => setPassword(e.target.value)} required
              className="w-full px-4 py-2.5 rounded-lg border text-base" />
          </div>
          <button type="submit" className="w-full rdmt-btn-primary py-3 rounded-lg text-base mt-6">
            Iniciar sesión
          </button>
        </form>
      </div>
    </div>
  );
}
