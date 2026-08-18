
import { z } from 'zod';

import { desktopCommands, getMonitors, onProviderEmit } from '~/desktop';
import { getCoordinateDistance } from '~/utils';
import { createBaseProvider } from '../create-base-provider';
import type {
  SwayOutput,
  SwayProvider,
  SwayProviderConfig,
} from '~/providers/sway/sway-provider-types';
import type {
  AudioOutput,
  AudioProvider,
  AudioProviderConfig,
  SetMuteOptions,
  SetVolumeOptions,
} from '~/providers';

const SwayProviderConfigSchema = z.object({
  type: z.literal('sway'),
});

export function createSwayProvider(
  config: SwayProviderConfig,
): SwayProvider {
  const mergedConfig = SwayProviderConfigSchema.parse(config);

  return createBaseProvider(mergedConfig, async queue => {
    const monitors = await getMonitors();
    return onProviderEmit<SwayOutput>(
      mergedConfig,
      ({ configHash, result }) => {
        if ('error' in result) {
          queue.error(result.error);
        } else {
          queue.output({
            ...result.output,
            runCommand(payload: string) {
              return desktopCommands.callProviderFunction(configHash, {
                type: 'sway',
                function: {
                  name: 'run_command',
                  args: { payload }
                }
              })
            },
          });
        }
      },
    );
  });
}