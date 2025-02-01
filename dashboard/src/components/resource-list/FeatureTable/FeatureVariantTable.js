// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { Typography } from '@mui/material';
import { useRouter } from 'next/router';
import React, { useCallback, useEffect, useState } from 'react';
import { useDataAPI } from '../../../hooks/dataAPI';
import { MainContainer, GridContainer, StyledDataGrid, STATUS_COLORS } from '../BaseColumnTable';
import BaseFilterPanel from '../BaseFilterPanel';
import { isMatchingDefault } from '../DatasetTable/DatasetTable';
import { ConnectionSvg } from '../icons/Connections';
import { UserBubbleSvg } from '../icons/Owner';
import NoDataMessage from '../NoDataMessage';
import FilterPanel from './FilterPanel';

export const feature_variant_columns = [
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
    flex: 1,
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
          <Typography variant='body2' sx={{ marginLeft: 1, fontSize: '0.75rem' }}>
          ({row?.variant})
        </Typography>
          </div>
        </div>
      );
    },
  },
  {
    field: 'owner',
    headerName: 'Owner',
    flex: 0,
    width: 350,
    editable: false,
    sortable: false,
    filterable: false,
    hide: false,
    renderCell: function ({ row }) {
      return (
        <div>
          <div style={{ display: 'flex' }}>
          <UserBubbleSvg
              height='20'
              width='20'
              letter={row?.owner?.[0]?.toUpperCase()}
            />
            <Typography variant='body2' sx={{ marginLeft: 1 }}>
              {row?.owner}
            </Typography>
          </div>
        </div>
      );
    },
  },
  {
    field: 'status',
    headerName: 'Status',
    flex: 0,
    width: 350,
    editable: false,
    sortable: false,
    filterable: false,
    renderCell: function ({ row }) {
      let result = STATUS_COLORS.ERROR;
      if (row?.status && row?.status === 'READY') {
        result = STATUS_COLORS.READY;
      }
      return (
          <div style={{ display: 'flex' }}>
            <ConnectionSvg fill={result} height='20' width='20' />
            <Typography variant='body2' sx={{ marginLeft: 1 }}>
              {row?.status}
            </Typography>
          </div>
      );
    },
  },
];

const DEFAULT_FILTERS = Object.freeze({
  SearchTxt: '',
  Owners: [],
  Statuses: [],
  Tags: [],
  pageSize: 10,
  offset: 0,
});

const CHECK_BOX_LIMIT = 8;

export const FeatureVariantTable = () => {
  const [tags, setTags] = useState([]);
  const [owners, setOwners] = useState([]);
  const [filters, setFilters] = useState({
    ...DEFAULT_FILTERS,
  });
  let router = useRouter();
  const [rows, setRows] = useState([]);
  const [loading, setIsLoading] = useState(false);
  const [totalRowCount, setTotalRowCount] = useState(0);

  const dataAPI = useDataAPI();

  useEffect(() => {
    const getResources = async () => {
      setIsLoading(true);
      try {
        let resp = await dataAPI.getFeatureVariants(filters);
        if (resp) {
          setRows(resp.data?.length ? resp.data : []);
          setTotalRowCount(resp.count);
        }
      } catch (error) {
        console.error('Error fetching features', error);
      } finally {
        setIsLoading(false);
      }
    };
    getResources();
  }, [filters]);

  useEffect(() => {
    const getCheckBoxLists = async () => {
      const [allTags, allOwners] = await Promise.all([
        dataAPI.getTypeTags('features'),
        dataAPI.getTypeOwners('features'),
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
      router.push(`/features/${name}?variant=${variant}`);
    }
  };

  return (
    <>
      <MainContainer>
        <BaseFilterPanel onTextFieldEnter={onTextFieldEnter}>
          <FilterPanel
            filters={filters}
            tags={tags}
            owners={owners}
            onCheckBoxChange={checkBoxFilterChange}
          />
        </BaseFilterPanel>
        <GridContainer>
          <h3>{'Features'}</h3>
          {loading ? (
            <div data-testid='loadingGrid'>
              <StyledDataGrid
                disableVirtualization
                aria-label={'Features'}
                autoHeight
                density='compact'
                loading={loading}
                rows={[]}
                rowCount={0}
                columns={feature_variant_columns}
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
              aria-label={'Feature'}
              rows={rows}
              columns={feature_variant_columns}
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
                    type={'Feature'}
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

export default FeatureVariantTable;
