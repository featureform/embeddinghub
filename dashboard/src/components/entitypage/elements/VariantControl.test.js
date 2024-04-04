import { ThemeProvider } from '@mui/material/styles';
import { configureStore, createSlice } from '@reduxjs/toolkit';
import { cleanup, fireEvent, render } from '@testing-library/react';
import 'jest-canvas-mock';
import React from 'react';
import ReduxWrapper from '../../../components/redux/wrapper/ReduxWrapper';
import TEST_THEME from '../../../styles/theme';
import VariantControl from './VariantControl';

describe('Variant Control Tests', () => {
  const SELECT_ID = 'variantControlSelectId';
  const DIV_NODE = 'DIV';
  const VARIANT_VIEW_ID = 'variantViewId';

  //unordered list, the postfix represents the expected order
  // by most recent: 0->4
  const RESOURCE_MOCK = {
    'all-variants': [
      'keen_wiles_3',
      'altruistic_hoover_1',
      'ecstatic_heyrovsky_2',
      'dedicated_poitras_0',
      'vigorous_heisenberg_4',
    ],
    variants: {
      altruistic_hoover_1: {
        variant: 'altruistic_hoover_1',
        created: '2020-11-01T23:08:25.301884462Z',
        owner: 'user@featureform.com',
      },
      dedicated_poitras_0: {
        variant: 'dedicated_poitras_0',
        created: '2023-10-31T20:23:30.728769926Z',
        owner: 'user@featureform.com',
      },
      ecstatic_heyrovsky_2: {
        variant: 'ecstatic_heyrovsky_2',
        created: '2015-11-02T16:46:46.601499508Z',
        owner: 'user@featureform.com',
      },
      keen_wiles_3: {
        variant: '0_keen_wiles_3',
        created: '2015-11-02T15:03:03.210467718Z',
        owner: 'user@featureform.com',
      },
      vigorous_heisenberg_4: {
        variant: 'vigorous_heisenberg_4',
        created: '2010-11-03T15:24:58.705404755Z',
        owner: 'user@featureform.com',
      },
    },
  };

  const mockChangeHandle = jest.fn();

  const getTestBody = (
    variant = RESOURCE_MOCK['all-variants'][0],
    variantListProp = RESOURCE_MOCK['all-variants'],
    resources = RESOURCE_MOCK,
    handleVariantChange = mockChangeHandle
  ) => {
    const slice = createSlice({
      name: 'testSlice',
      initialState: {},
    });
    const store = configureStore({
      reducer: slice.reducer,
    });
    return (
      <>
        <ReduxWrapper store={store}>
          <ThemeProvider theme={TEST_THEME}>
            <VariantControl
              variant={variant}
              variantListProp={variantListProp}
              resources={resources}
              handleVariantChange={handleVariantChange}
            />
          </ThemeProvider>
        </ReduxWrapper>
      </>
    );
  };

  beforeEach(() => {
    jest.resetAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  test('Basic render displays all options', async () => {
    //given:
    const helper = render(getTestBody());

    //when:
    const dropdown = helper.getByTestId(SELECT_ID);
    fireEvent.click(dropdown);
    const foundVariantView = helper.getByTestId(VARIANT_VIEW_ID);

    //then:
    expect(foundVariantView).toBeDefined();

    RESOURCE_MOCK['all-variants'].map((vr) => {
      const foundVR = RESOURCE_MOCK.variants[vr];
      const foundItem = helper.getByText(foundVR.variant);
      expect(foundItem.nodeName).toBe(DIV_NODE);
    });
  });

  test('Selecting an item fires off the handleChange handler', async () => {
    //given:
    const helper = render(getTestBody());
    const variantKey = RESOURCE_MOCK['all-variants'][0];
    const variantName = RESOURCE_MOCK.variants[variantKey].variant;

    //when:
    const dropdown = helper.getByTestId(SELECT_ID);
    fireEvent.click(dropdown);

    const item = helper.getByText(variantName);
    fireEvent.click(item);

    //then:
    expect(mockChangeHandle).toHaveBeenCalledTimes(1);
    expect(mockChangeHandle).toHaveBeenCalledWith(variantName);
  });

  test('An invalid variant date still renders the passed in prop list', async () => {
    //given: a resource mock with an intentional wrong variants prop
    console.error = jest.fn();
    const MOCK_COPY = { ...RESOURCE_MOCK };
    const variantKey = MOCK_COPY['all-variants'][0];
    MOCK_COPY.variants[variantKey].created = 'hahaha';

    const allVariants = MOCK_COPY['all-variants'];
    const helper = render(getTestBody('placeholder', allVariants, MOCK_COPY));

    //when:
    const dropdown = helper.getByTestId(SELECT_ID);
    fireEvent.click(dropdown);

    const foundVariantView = helper.getByTestId(VARIANT_VIEW_ID);

    //then: an error was called but the list renders OK
    expect(console.error).toHaveBeenCalledTimes(1);
    expect(foundVariantView.nodeName).toBeDefined();
    allVariants.map((vr) => {
      const foundVR = RESOURCE_MOCK.variants[vr];
      const foundItem = helper.getByText(foundVR.variant);
      expect(foundItem.nodeName).toBe(DIV_NODE);
    });
  });
});
