// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { cleanup, fireEvent, render } from '@testing-library/react';
import 'jest-canvas-mock';
import React from 'react';
import SearchTable from './SearchTable';
import { search_results } from './testData/test_results';

const dataApiMock = {
  searchResources: jest.fn().mockResolvedValue(search_results),
};

jest.mock('../../hooks/dataAPI', () => ({
  useDataAPI: () => dataApiMock,
}));

const userRouterMock = {
  push: jest.fn(),
  query: {q: ''},
};

jest.mock('next/router', () => ({
  useRouter: () => userRouterMock,
}));

describe('Search Table Tests', () => {
  const getTestBody = () => {
    return (
      <>
        <SearchTable />
      </>
    );
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  test('Basic Table Renders without any rows', async () => {
    //given:
    const helper = render(
      getTestBody()
    );
    console.log(document.body.innerHTML);
    const foundText = helper.getByText(`Search Results:`);

    //expect:
    expect(foundText).toBeDefined();
  });

  test('Basic table renders each resource row', async () => {
    //given:
    const helper = render(getTestBody());

    //then:
    search_results.forEach(async (element) => {
      const foundTitle = await helper.findByText(element.Name);
      expect(foundTitle).toBeDefined();
    });
  });

  test.each`
    ResourceParam             | RouteParam
    ${'SOURCE'}              | ${'/sources/average_user_transaction?variant=2024-04-17t14-59-42'}
    ${'SOURCE_VARIANT'}      | ${'/sources/average_user_transaction_dev?variant=2024-04-22t18-11-31'}
    ${'FEATURE'}             | ${'/features/ondemand_add_integers?variant=staging test'}
    ${'FEATURE_VARIANT'}     | ${'/features/ondemand_add_integers_dev?variant=2024-04-17t13-49-05'}
    ${'USER'}                | ${'/users/a_user'}
    ${'ENTITY'}              | ${'/entities/user'}
    ${'TRAINING_SET_VARIANT'}| ${'/training-sets/fraud?variant=2024-04-23t14-33-15'}
    ${'LABEL'}               | ${'/labels/my_label?variant=2024-04-23t14-33-15'}
    ${'LABEL_VARIANT'}       | ${'/labels/my_label_variant?variant=2024-04-23t14-33-15'}
    ${'MODEL'}               | ${'/models/my_model'}
  `(
    'Clicking a resource row of type ($ResourceParam) routes to the correct address',
    async ({ ResourceParam, RouteParam }) => {
      const rowItem = search_results.find(q => q.Type === ResourceParam)
      // when:
      const helper = render(<SearchTable />);
      await helper.findByText("Filters");
      const row = helper.getByText(rowItem.Name);
      fireEvent.click(row);

      // then:
      expect(dataApiMock.searchResources).toHaveBeenCalledTimes(2);      
      expect(userRouterMock.push).toHaveBeenCalledTimes(1);
      expect(userRouterMock.push).toHaveBeenCalledWith(RouteParam);
    }
  );
});
