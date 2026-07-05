import { createContext, useContext } from 'react';

export type ComposeWizardStep = 'build' | 'review' | 'deliver';

export interface ComposeStepDescriptor {
  id: ComposeWizardStep;
  label: string;
  detail: string;
}

export interface ComposeStepState {
  currentStep: ComposeWizardStep;
  completedSteps: ComposeWizardStep[];
  availableSteps: ComposeWizardStep[];
}

export interface ComposeStepNavigation extends ComposeStepState {
  setCurrentStep: (step: ComposeWizardStep) => void;
  updateStepNavigation: (state: Partial<ComposeStepState>) => void;
}

export const composeWizardSteps: ComposeStepDescriptor[] = [
  { id: 'build', label: 'Ingest', detail: 'Source and sample' },
  { id: 'review', label: 'Transform', detail: 'Processing logic' },
  { id: 'deliver', label: 'Destination', detail: 'Export target' },
];

export const defaultComposeStepState: ComposeStepState = {
  currentStep: 'build',
  completedSteps: [],
  availableSteps: ['build'],
};

export const ComposeStepNavigationContext = createContext<ComposeStepNavigation>({
  ...defaultComposeStepState,
  setCurrentStep: () => undefined,
  updateStepNavigation: () => undefined,
});

export function useComposeStepNavigation(): ComposeStepNavigation {
  return useContext(ComposeStepNavigationContext);
}
