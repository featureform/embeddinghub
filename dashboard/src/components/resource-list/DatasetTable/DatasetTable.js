// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import Typography from '@mui/material/Typography';
import { useRouter } from 'next/router';
import React, { useCallback, useEffect, useState } from 'react';
import { useDataAPI } from '../../../hooks/dataAPI';
import {
  GridContainer,
  MainContainer,
  StyledDataGrid,
  STATUS_COLORS,
} from '../BaseColumnTable';
import BaseFilterPanel from '../BaseFilterPanel';
import { ConnectionSvg } from '../icons/Connections';
import { UserBubbleSvg } from '../icons/Owner';
import NoDataMessage from '../NoDataMessage';
import FilterPanel from './FilterPanel';

export const dataset_columns = [
  {
    field: 'id',
    headerName: 'id',
    flex: 1,
    editable: false,
    sortable: false,
    filterable: false,
    hide: true,
  },
  {
    field: 'name',
    headerName: 'Name (Latest Variant)',
    width: 200,
    editable: false,
    sortable: false,
    filterable: false,
    hide: false,
    renderCell: function ({ row }) {
      return (
        <div>
          <div style={{ display: 'flex' }}>
            <Typography variant='body2' sx={{ marginLeft: 1 }}>
                <strong>{row?.name}</strong>
              </Typography>
          </div>

          <div style={{ display: 'flex' }}>
            <Typography variant='body2' sx={{ marginLeft: 1 }}>
              {row?.variant}
            </Typography>
          </div>
        </div>
      );
    },
  },
  {
    field: 'sourceType',
    headerName: 'Dataset Type',
    width: 125,
    editable: false,
    sortable: false,
    filterable: false,
    renderCell: function ({ row }) {
      return (
        <Typography variant='body1' sx={{ marginLeft: 1 }}>
          {row?.['source-type']?.split(' ')?.[0] ?? '---'}
        </Typography>
      );
    },
  },
  {
    field: 'mode',
    headerName: 'Mode',
    editable: false,
    sortable: false,
    filterable: false,
    renderCell: function () {
      return (
        <Typography variant='body1' sx={{ marginLeft: 1 }}>
          ---
        </Typography>
      );
    },
  },
  {
    field: 'status',
    headerName: 'Status',
    width: 250,
    editable: false,
    sortable: false,
    filterable: false,
    renderCell: function ({ row }) {
      let result = STATUS_COLORS.ERROR;
      if (row?.status && row?.status === 'READY') {
        result = STATUS_COLORS.READY;
      }
      return (
        <div>
          <div style={{ display: 'flex' }}>
            <ConnectionSvg fill={result} height='20' width='20' />
            <Typography variant='body1' sx={{ marginLeft: 1 }}>
              {row?.status}
            </Typography>
          </div>
          <div style={{ display: 'flex' }}>
            <UserBubbleSvg
              height='20'
              width='20'
              letter={row?.owner?.[0]?.toUpperCase()}
            />
            <Typography variant='body1' sx={{ marginLeft: 1 }}>
              {row?.owner}
            </Typography>
          </div>
        </div>
      );
    },
  },
  {
    field: 'lastUpdated',
    headerName: 'Last Update',
    hide: true,
    sortable: false,
    filterable: false,
    width: 100,
    valueGetter: ({ row }) => {
      return new Date(row?.lastUpdated)?.toLocaleDateString()?.split(' ')?.[0];
    },
  },
];

const DEFAULT_FILTERS = Object.freeze({
  SearchTxt: '',
  Owners: [],
  Modes: [],
  Statuses: [],
  Types: [],
  Tags: [],
  pageSize: 10,
  offset: 0,
});

export function isMatchingDefault(default_filter, obj) {
  for (let key in default_filter) {
    const defaultValue = default_filter[key];
    const objValue = obj[key];

    if (Array.isArray(defaultValue)) {
      if (!Array.isArray(objValue) || defaultValue.length !== objValue.length) {
        return false;
      }

      for (let i = 0; i < defaultValue.length; i++) {
        if (defaultValue[i] !== objValue[i]) {
          return false;
        }
      }
    } else {
      if (defaultValue !== objValue) {
        return false;
      }
    }
  }

  //everything matches, return true
  return true;
}

const CHECK_BOX_LIMIT = 8;

export const DatasetTable = () => {
  let router = useRouter();
  const [tags, setTags] = useState([]);
  const [owners, setOwners] = useState([]);
  const [filters, setFilters] = useState({
    ...DEFAULT_FILTERS,
  });
  const [rows, setRows] = useState([]);
  const [loading, setIsLoading] = useState(false);
  const [totalRowCount, setTotalRowCount] = useState(0);

  const dataAPI = useDataAPI();

  useEffect(() => {
    const getResources = async () => {
      setIsLoading(true);
      try {
        let resp = await dataAPI.getSourceVariants(filters);
        if (resp) {
          setRows(resp.data?.length ? resp.data : []);
          setTotalRowCount(resp.count);
        }
      } catch (error) {
        console.error('Error fetching sources', error);
      } finally {
        setIsLoading(false);
      }
    };
    getResources();
  }, [filters]);

  useEffect(() => {
    const getCheckBoxLists = async () => {
      const [allTags, allOwners] = await Promise.all([
        dataAPI.getTypeTags('sources'),
        dataAPI.getTypeOwners('sources'),
      ]);

      if (allTags) {
        setTags(allTags.slice(0, CHECK_BOX_LIMIT));
      }

      if (allOwners) {
        setOwners(allOwners.slice(0, CHECK_BOX_LIMIT));
      }
    };
    getCheckBoxLists();
  }, []);

  const checkBoxFilterChange = (key, value) => {
    const updateFilters = { ...filters };
    const list = updateFilters[key];
    const index = list.indexOf(value);

    if (index === -1) {
      list.push(value);
    } else {
      list.splice(index, 1);
    }
    setFilters(updateFilters);
  };

  const typeBoxFilterChange = (key, value) => {
    const updateFilters = { ...filters };
    const list = updateFilters[key];

    if (list.includes(value)) {
      updateFilters[key] = [];
    } else {
      updateFilters[key] = [value];
    }

    setFilters(updateFilters);
  };

  const onTextFieldEnter = useCallback(
    (value = '') => {
      const updateFilters = { ...filters, SearchTxt: value };
      setFilters(updateFilters);
    },
    [filters]
  );

  const handlePageChange = useCallback(
    (newPage) => {
      const updatedFilters = { ...filters, offset: newPage };
      setFilters(updatedFilters);
    },
    [filters]
  );

  const redirect = (name = '', variant = '') => {
    if (name && variant) {
      router.push(`/sources/${name}?variant=${variant}`);
    }
  };

  return (
    <>
      <MainContainer>
        <BaseFilterPanel onTextFieldEnter={onTextFieldEnter}>
          <FilterPanel
            filters={filters}
            owners={owners}
            tags={tags}
            onCheckBoxChange={checkBoxFilterChange}
            onTypeBoxChange={typeBoxFilterChange}
          />
        </BaseFilterPanel>

        <GridContainer>
          <h3>{'Datasets'}</h3>
          {loading ? (
            <div data-testid='loadingGrid'>
              <StyledDataGrid
                disableVirtualization
                aria-label={'Datasets'}
                autoHeight
                density='compact'
                loading={loading}
                rows={[]}
                rowCount={0}
                columns={dataset_columns}
                hideFooterSelectedRowCount
                disableColumnFilter
                disableColumnMenu
                disableColumnSelector
                paginationMode='server'
                rowsPerPageOptions={[filters.pageSize]}
              />
            </div>
          ) : (
            <StyledDataGrid
              disableVirtualization
              aria-label={'Datasets'}
              rows={rows}
              columns={dataset_columns}
              density='compact'
              rowHeight={80}
              hideFooterSelectedRowCount
              disableColumnFilter
              disableColumnMenu
              disableColumnSelector
              paginationMode='server'
              sortModel={[{ field: 'name', sort: 'asc' }]}
              onPageChange={(newPage) => {
                handlePageChange(newPage);
              }}
              components={{
                NoRowsOverlay: () => (
                  <NoDataMessage
                    type={'Source'}
                    usingFilters={!isMatchingDefault(DEFAULT_FILTERS, filters)}
                  />
                ),
              }}
              rowCount={totalRowCount}
              page={filters.offset}
              pageSize={filters.pageSize}
              rowsPerPageOptions={[filters.pageSize]}
              onRowClick={(params, event) => {
                event?.preventDefault();
                if (params?.row?.name) {
                  redirect(params.row.name, params.row.variant);
                }
              }}
              getRowId={(row) => row.variant + row.name}
            />
          )}
        </GridContainer>
      </MainContainer>
    </>
  );
};

export default DatasetTable;
