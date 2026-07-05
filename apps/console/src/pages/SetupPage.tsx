import { useEffect, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useMediaQuery } from '@astryxdesign/core/hooks';
import { Badge, type BadgeVariant } from '@astryxdesign/core/Badge';
import { Banner } from '@astryxdesign/core/Banner';
import { Button } from '@astryxdesign/core/Button';
import { ButtonGroup } from '@astryxdesign/core/ButtonGroup';
import { CheckboxInput } from '@astryxdesign/core/CheckboxInput';
import { Divider } from '@astryxdesign/core/Divider';
import { FormLayout } from '@astryxdesign/core/FormLayout';
import { Grid } from '@astryxdesign/core/Grid';
import { HStack } from '@astryxdesign/core/HStack';
import { Heading } from '@astryxdesign/core/Heading';
import { Icon } from '@astryxdesign/core/Icon';
import { List, ListItem } from '@astryxdesign/core/List';
import { MetadataList, MetadataListItem } from '@astryxdesign/core/MetadataList';
import { Section } from '@astryxdesign/core/Section';
import { StackItem } from '@astryxdesign/core/Stack';
import { Tab, TabList } from '@astryxdesign/core/TabList';
import { Text } from '@astryxdesign/core/Text';
import { TextInput } from '@astryxdesign/core/TextInput';
import { VStack } from '@astryxdesign/core/VStack';
import { CircleStackIcon, CommandLineIcon, SparklesIcon } from '@heroicons/react/24/outline';
import { LoadingBlock, PageHeader } from '../components/ui';
import { api } from '../lib/api';
import type { AiProviderSettings, OidcSettings } from '../lib/api-types';
import { describeConsoleError } from '../lib/error-state';

type SettingsTab = 'ai' | 'oidc';

function providerVariant(activeProvider: string): BadgeVariant {
  return activeProvider === 'gemini' ? 'success' : 'warning';
}

function sourceVariant(source: string): BadgeVariant {
  return source === 'database' ? 'success' : 'warning';
}

function sourceLabel(source: string): string {
  switch (source) {
    case 'database':
      return 'Saved in database';
    case 'environment':
      return 'Environment fallback';
    default:
      return 'Not configured';
  }
}

export function SetupPage() {
  const queryClient = useQueryClient();
  const isNarrow = useMediaQuery('(max-width: 768px)');
  const settingsQuery = useQuery({ queryKey: ['ai-settings'], queryFn: api.fetchAiSettings });
  const oidcQuery = useQuery({ queryKey: ['oidc-settings'], queryFn: api.fetchOidcSettings });
  const [activeTab, setActiveTab] = useState<SettingsTab>('ai');
  const [enabled, setEnabled] = useState(false);
  const [model, setModel] = useState('gemini-2.5-flash');
  const [apiBaseUrl, setApiBaseUrl] = useState('https://generativelanguage.googleapis.com/v1beta');
  const [apiKey, setApiKey] = useState('');
  const [clearApiKey, setClearApiKey] = useState(false);

  useEffect(() => {
    const settings = settingsQuery.data;
    if (!settings) return;
    setEnabled(settings.enabled);
    setModel(settings.model);
    setApiBaseUrl(settings.apiBaseUrl);
    setApiKey('');
    setClearApiKey(false);
  }, [settingsQuery.data]);

  const saveMutation = useMutation({
    mutationFn: () =>
      api.saveAiSettings({
        enabled,
        model,
        apiBaseUrl,
        apiKey: apiKey.trim() || undefined,
        clearApiKey,
      }),
    onSuccess: async () => {
      setApiKey('');
      setClearApiKey(false);
      await queryClient.invalidateQueries({ queryKey: ['ai-settings'] });
    },
  });

  const signOutMutation = useMutation({
    mutationFn: api.signOutOidc,
    onSuccess: () => {
      window.location.reload();
    },
  });

  if (settingsQuery.isPending || oidcQuery.isPending) {
    return (
      <VStack gap={5}>
        <PageHeader eyebrow="Setup" title="Settings" sub="Loading provider settings." />
        <Section>
          <LoadingBlock lines={5} />
        </Section>
      </VStack>
    );
  }

  if (settingsQuery.isError || oidcQuery.isError) {
    const errorState = describeConsoleError(settingsQuery.error ?? oidcQuery.error);
    return (
      <VStack gap={5}>
        <PageHeader eyebrow="Setup" title="Settings unavailable" />
        <Banner status="error" title={errorState.message} description={errorState.hint} />
      </VStack>
    );
  }

  const settings = settingsQuery.data!;
  const oidcSettings = oidcQuery.data!;
  const canSave = !saveMutation.isPending && Boolean(model.trim()) && Boolean(apiBaseUrl.trim());

  function resetDraft() {
    setEnabled(settings.enabled);
    setModel(settings.model);
    setApiBaseUrl(settings.apiBaseUrl);
    setApiKey('');
    setClearApiKey(false);
  }

  return (
    <VStack gap={5}>
      <PageHeader
        eyebrow="Setup"
        title="Settings"
        sub="Configure authoring intelligence and optional console sign-in."
        actions={
          <Badge
            variant={oidcSettings.enabled ? 'success' : providerVariant(settings.activeProvider)}
            label={oidcSettings.enabled ? 'OIDC enforced' : settings.activeProvider === 'gemini' ? 'Gemini active' : 'Heuristic fallback'}
          />
        }
      />

      {isNarrow ? (
        <TabList value={activeTab} onChange={(value) => setActiveTab(value as SettingsTab)} hasDivider>
          <Tab value="ai" label="AI provider" />
          <Tab
            value="oidc"
            label="OIDC"
            endContent={<Badge variant={oidcSettings.enabled ? 'success' : 'neutral'} label={oidcSettings.enabled ? 'On' : 'Off'} />}
          />
        </TabList>
      ) : null}

      <HStack gap={5} align="start">
        {isNarrow ? null : (
          <VStack gap={2} width={240}>
            <List density="balanced">
              <ListItem
                label="AI provider"
                description="Gemini setup and Compose behavior"
                isSelected={activeTab === 'ai'}
                onClick={() => setActiveTab('ai')}
                endContent={<Badge variant={providerVariant(settings.activeProvider)} label={settings.activeProvider === 'gemini' ? 'On' : 'Fallback'} />}
              />
              <ListItem
                label="OIDC"
                description="Console sign-in enforcement"
                isSelected={activeTab === 'oidc'}
                onClick={() => setActiveTab('oidc')}
                endContent={<Badge variant={oidcSettings.enabled ? 'success' : 'neutral'} label={oidcSettings.enabled ? 'On' : 'Off'} />}
              />
            </List>
          </VStack>
        )}

        <StackItem size="fill">
          {activeTab === 'oidc' ? (
            <OidcSettingsPanel
              settings={oidcSettings}
              isSigningOut={signOutMutation.isPending}
              onSignOut={() => signOutMutation.mutate()}
            />
          ) : (
            <AiSettingsPanel
              settings={settings}
              enabled={enabled}
              model={model}
              apiBaseUrl={apiBaseUrl}
              apiKey={apiKey}
              clearApiKey={clearApiKey}
              canSave={canSave}
              isSaving={saveMutation.isPending}
              saveError={saveMutation.error}
              isSaved={saveMutation.isSuccess}
              onEnabledChange={setEnabled}
              onModelChange={setModel}
              onApiBaseUrlChange={setApiBaseUrl}
              onApiKeyChange={(value) => {
                setApiKey(value);
                if (value.trim()) setClearApiKey(false);
              }}
              onClearApiKeyChange={(checked) => {
                setClearApiKey(checked);
                if (checked) setApiKey('');
              }}
              onReset={resetDraft}
              onSave={() => saveMutation.mutate()}
            />
          )}
        </StackItem>
      </HStack>
    </VStack>
  );
}

function AiSettingsPanel({
  settings,
  enabled,
  model,
  apiBaseUrl,
  apiKey,
  clearApiKey,
  canSave,
  isSaving,
  saveError,
  isSaved,
  onEnabledChange,
  onModelChange,
  onApiBaseUrlChange,
  onApiKeyChange,
  onClearApiKeyChange,
  onReset,
  onSave,
}: {
  settings: AiProviderSettings;
  enabled: boolean;
  model: string;
  apiBaseUrl: string;
  apiKey: string;
  clearApiKey: boolean;
  canSave: boolean;
  isSaving: boolean;
  saveError: Error | null;
  isSaved: boolean;
  onEnabledChange: (value: boolean) => void;
  onModelChange: (value: string) => void;
  onApiBaseUrlChange: (value: string) => void;
  onApiKeyChange: (value: string) => void;
  onClearApiKeyChange: (value: boolean) => void;
  onReset: () => void;
  onSave: () => void;
}) {
  return (
    <VStack gap={5} maxWidth={920}>
      <Grid columns={{ minWidth: 300, max: 2 }} gap={10}>
        <VStack gap={1}>
          <Heading level={3}>Provider status</Heading>
          <Text type="supporting" color="secondary" display="block">
            Check which planner Compose will use before changing configuration.
          </Text>
        </VStack>
        <MetadataList columns="single" label={{ position: 'start', width: 120 }}>
          <MetadataListItem label="Runtime">
            <Text type="body" display="block">
              {settings.activeProvider === 'gemini' ? 'Gemini AI planning' : 'Local heuristic planning'}
            </Text>
          </MetadataListItem>
          <MetadataListItem label="Model">
            <Text type="body" display="block">{settings.model}</Text>
          </MetadataListItem>
          <MetadataListItem label="Token">
            <Badge
              variant={settings.apiKeyConfigured ? 'success' : 'warning'}
              label={settings.apiKeyConfigured ? 'Configured' : 'Missing'}
            />
          </MetadataListItem>
          <MetadataListItem label="Source">
            <Badge variant={sourceVariant(settings.source)} label={sourceLabel(settings.source)} />
          </MetadataListItem>
        </MetadataList>
      </Grid>

      <Divider />

      <form
        onSubmit={(event) => {
          event.preventDefault();
          if (canSave) onSave();
        }}
      >
        <Grid columns={{ minWidth: 300, max: 2 }} gap={10}>
          <VStack gap={1}>
            <Heading level={3}>Gemini configuration</Heading>
            <Text type="supporting" color="secondary" display="block">
              The control API stores the token and does not send it back to the browser.
            </Text>
          </VStack>
          <VStack gap={4}>
            <FormLayout>
              <CheckboxInput
                label="Use AI for Compose"
                description="When disabled or missing a token, Compose stays on the local heuristic planner."
                value={enabled}
                onChange={onEnabledChange}
              />

              <TextInput
                label="Model"
                value={model}
                onChange={onModelChange}
                placeholder="gemini-2.5-flash"
                htmlName="model"
                isRequired
              />

              <TextInput
                label="API base URL"
                value={apiBaseUrl}
                onChange={onApiBaseUrlChange}
                placeholder="https://generativelanguage.googleapis.com/v1beta"
                htmlName="apiBaseUrl"
                isRequired
              />

              <TextInput
                label="API token"
                type="password"
                value={apiKey}
                onChange={onApiKeyChange}
                placeholder={settings.apiKeyConfigured ? 'Token saved. Enter a new token to replace it.' : 'Paste Gemini API token'}
                htmlName="apiKey"
                isOptional
              />

              {settings.apiKeyConfigured ? (
                <CheckboxInput
                  label="Clear saved token on save"
                  description="Leaves the configured token empty until a new one is saved."
                  value={clearApiKey}
                  onChange={onClearApiKeyChange}
                />
              ) : null}

              <ButtonGroup label="AI setup actions">
                <Button
                  label="Reset changes"
                  variant="secondary"
                  type="button"
                  onClick={onReset}
                  isDisabled={isSaving}
                />
                <Button
                  label="Save AI setup"
                  variant="secondary"
                  type="submit"
                  isLoading={isSaving}
                  isDisabled={!canSave}
                />
              </ButtonGroup>
            </FormLayout>

            {saveError ? (
              <Banner status="error" title="Could not save AI setup" description={saveError.message} />
            ) : null}
            {isSaved ? (
              <Banner status="success" title="AI setup saved." isDismissable />
            ) : null}
          </VStack>
        </Grid>
      </form>

      <Divider />

      <Grid columns={{ minWidth: 300, max: 2 }} gap={10}>
        <VStack gap={1}>
          <Heading level={3}>Compose behavior</Heading>
          <Text type="supporting" color="secondary" display="block">
            Compose reports the provider used for every generated plan.
          </Text>
        </VStack>
        <List density="balanced" hasDividers>
          <ListItem
            label="Gemini active"
            description="Transforms are planned by the configured model."
            startContent={<Icon icon={SparklesIcon} size="sm" />}
          />
          <ListItem
            label="Heuristic fallback"
            description="Transforms are generated locally from deterministic rules."
            startContent={<Icon icon={CommandLineIcon} size="sm" />}
          />
          <ListItem
            label="Database token"
            description="The control API stores the token and only returns configured status."
            startContent={<Icon icon={CircleStackIcon} size="sm" />}
          />
        </List>
      </Grid>
    </VStack>
  );
}

function OidcSettingsPanel({
  settings,
  isSigningOut,
  onSignOut,
}: {
  settings: OidcSettings;
  isSigningOut: boolean;
  onSignOut: () => void;
}) {
  return (
    <VStack gap={5} maxWidth={920}>
      <Grid columns={{ minWidth: 300, max: 2 }} gap={10}>
        <VStack gap={1}>
          <HStack gap={2} align="center">
            <Heading level={3}>OIDC sign-in</Heading>
            <Badge variant={settings.enabled ? 'success' : 'neutral'} label={settings.enabled ? 'Enforced' : 'Not configured'} />
          </HStack>
          <Text type="supporting" color="secondary" display="block">
            {settings.enabled
              ? 'Console users must sign in through the configured provider before the app loads.'
              : 'The console keeps using the existing API token flow until OIDC is configured.'}
          </Text>
        </VStack>
        <VStack gap={4}>
          <MetadataList columns="single" label={{ position: 'start', width: 140 }}>
            <MetadataListItem label="Mode">
              <Badge variant={settings.loginRequired ? 'success' : 'neutral'} label={settings.loginRequired ? 'OIDC login' : 'API token'} />
            </MetadataListItem>
            <MetadataListItem label="Issuer">
              <Text type="body" display="block">{settings.issuerUrl ?? 'Not set'}</Text>
            </MetadataListItem>
            <MetadataListItem label="Client ID">
              <Text type="body" display="block">{settings.clientId ?? 'Not set'}</Text>
            </MetadataListItem>
            <MetadataListItem label="Scopes">
              <Text type="body" display="block">{settings.scope ?? 'openid profile email'}</Text>
            </MetadataListItem>
            <MetadataListItem label="Authorization URL">
              <Text type="body" display="block">{settings.authorizationEndpoint ?? 'Not discovered'}</Text>
            </MetadataListItem>
          </MetadataList>
          {settings.enabled ? (
            <ButtonGroup label="OIDC session actions">
              <Button
                label="Sign out"
                variant="secondary"
                type="button"
                isLoading={isSigningOut}
                onClick={onSignOut}
              />
            </ButtonGroup>
          ) : null}
        </VStack>
      </Grid>

      <Divider />

      <Grid columns={{ minWidth: 300, max: 2 }} gap={10}>
        <VStack gap={1}>
          <Heading level={3}>Environment</Heading>
          <Text type="supporting" color="secondary" display="block">
            Restart the control API after setting these values.
          </Text>
        </VStack>
        <List density="balanced" hasDividers>
          <ListItem label="CONTROL_API_OIDC_ISSUER_URL" description="OIDC issuer base URL." />
          <ListItem label="CONTROL_API_OIDC_CLIENT_ID" description="Console application client ID." />
          <ListItem label="CONTROL_API_OIDC_CLIENT_SECRET" description="Optional secret for confidential clients." />
          <ListItem label="CONTROL_API_OIDC_SCOPE" description="Optional scope override." />
        </List>
      </Grid>
    </VStack>
  );
}
