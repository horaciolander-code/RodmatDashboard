import { createContext, useContext, useState, useEffect, useCallback, type ReactNode } from 'react';
import api from '../services/api';

interface User {
  id: string;
  email: string;
  store_id: string;
  store_name?: string;
  role: string;
}

export interface StoreOption {
  id: string;
  name: string;
  owner_email: string;
}

interface AuthContextType {
  user: User | null;
  loading: boolean;
  isSuperadmin: boolean;
  isWarehouse: boolean;
  activeStoreId: string;
  activeStoreName: string;
  stores: StoreOption[];
  setActiveStore: (store: StoreOption) => void;
  login: (email: string, password: string) => Promise<string | null>;
  register: (email: string, password: string, storeName: string) => Promise<boolean>;
  logout: () => void;
}

const AuthContext = createContext<AuthContextType | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);
  const [stores, setStores] = useState<StoreOption[]>([]);
  const [activeStore, setActiveStoreState] = useState<StoreOption | null>(null);

  const isSuperadmin = user?.role === 'superadmin';
  const isWarehouse  = user?.role === 'warehouse';
  const activeStoreId = activeStore?.id ?? user?.store_id ?? '';
  const activeStoreName = activeStore?.name ?? '';

  const loadStores = useCallback(async () => {
    try {
      const res = await api.get('/admin/stores/all');
      setStores(res.data);
      if (res.data.length > 0) {
        const preferred = res.data.find((s: StoreOption) => s.name === 'Rodmat') ?? res.data[0];
        setActiveStoreState((prev) => prev ?? preferred);
      }
    } catch {
      // not superadmin or network error
    }
  }, []);

  useEffect(() => {
    const token = localStorage.getItem('jwt_token');
    if (token) {
      api.get('/auth/me')
        .then(async (res) => {
          setUser(res.data); localStorage.setItem('user_info', JSON.stringify(res.data)); if (res.data?.store_name) document.title = res.data.store_name + ' Dashboard';
          if (res.data.role === 'superadmin') {
            await loadStores();
          }
        })
        .catch(() => localStorage.removeItem('jwt_token'))
        .finally(() => setLoading(false));
    } else {
      setLoading(false);
    }
  }, []);

  const login = async (email: string, password: string): Promise<string | null> => {
    try {
      const form = new URLSearchParams();
      form.append('username', email);
      form.append('password', password);
      const res = await api.post('/auth/login', form, {
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      });
      localStorage.setItem('jwt_token', res.data.access_token);
      const me = await api.get('/auth/me');
      setUser(me.data); localStorage.setItem('user_info', JSON.stringify(me.data)); if (me.data?.store_name) document.title = me.data.store_name + ' Dashboard';
      if (me.data.role === 'superadmin') {
        await loadStores();
      }
      return me.data.role as string;
    } catch {
      return null;
    }
  };

  const register = async (email: string, password: string, storeName: string): Promise<boolean> => {
    try {
      const res = await api.post('/auth/register', { email, password, store_name: storeName });
      localStorage.setItem('jwt_token', res.data.access_token);
      const me = await api.get('/auth/me');
      setUser(me.data); localStorage.setItem('user_info', JSON.stringify(me.data)); if (me.data?.store_name) document.title = me.data.store_name + ' Dashboard';
      return true;
    } catch {
      return false;
    }
  };

  const logout = () => {
    localStorage.removeItem('jwt_token');
    setUser(null); localStorage.removeItem('user_info'); document.title = 'Dashboard';
    setStores([]);
    setActiveStoreState(null);
  };

  return (
    <AuthContext.Provider value={{
      user, loading, isSuperadmin, isWarehouse,
      activeStoreId, activeStoreName,
      stores, setActiveStore: setActiveStoreState,
      login, register, logout,
    }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used within AuthProvider');
  return ctx;
}
