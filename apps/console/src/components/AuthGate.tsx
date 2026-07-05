import { useEffect, useState, type ReactNode } from 'react';
import { Banner } from '@astryxdesign/core/Banner';
import { Spinner } from '@astryxdesign/core/Spinner';
import { Text } from '@astryxdesign/core/Text';
import { VStack } from '@astryxdesign/core/VStack';
import { fetchAuthSession, fetchOidcConfig, startOidcLogin } from '../lib/auth';

export function AuthGate({ children }: { children: ReactNode }) {
  const [state, setState] = useState<'loading' | 'ready' | 'redirecting'>('loading');
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function checkAuth() {
      try {
        const config = await fetchOidcConfig();
        if (!config.enabled) {
          if (!cancelled) setState('ready');
          return;
        }

        const session = await fetchAuthSession();
        if (session.authenticated) {
          if (!cancelled) setState('ready');
          return;
        }

        if (!cancelled) setState('redirecting');
        await startOidcLogin(config);
      } catch (authError) {
        if (!cancelled) {
          setError(authError instanceof Error ? authError.message : 'Authentication failed.');
          setState('loading');
        }
      }
    }

    void checkAuth();
    return () => {
      cancelled = true;
    };
  }, []);

  if (state === 'ready') return <>{children}</>;

  return (
    <VStack gap={3} padding={5} minHeight="100vh" justify="center" align="center">
      {error ? (
        <Banner status="error" title="Could not start login" description={error} />
      ) : (
        <>
          <Spinner size="md" />
          <Text type="supporting" color="secondary">
            {state === 'redirecting' ? 'Redirecting to sign in.' : 'Checking sign-in.'}
          </Text>
        </>
      )}
    </VStack>
  );
}
