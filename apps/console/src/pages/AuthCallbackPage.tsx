import { useEffect, useState } from 'react';
import { Banner } from '@astryxdesign/core/Banner';
import { Spinner } from '@astryxdesign/core/Spinner';
import { Text } from '@astryxdesign/core/Text';
import { VStack } from '@astryxdesign/core/VStack';
import { completeOidcLogin } from '../lib/auth';

export function AuthCallbackPage() {
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function finishLogin() {
      try {
        const returnTo = await completeOidcLogin();
        window.location.replace(returnTo);
      } catch (authError) {
        setError(authError instanceof Error ? authError.message : 'Login failed.');
      }
    }

    void finishLogin();
  }, []);

  return (
    <VStack gap={3} padding={5} minHeight="100vh" justify="center" align="center">
      {error ? (
        <Banner status="error" title="Could not finish login" description={error} />
      ) : (
        <>
          <Spinner size="md" />
          <Text type="supporting" color="secondary">Finishing sign-in.</Text>
        </>
      )}
    </VStack>
  );
}
