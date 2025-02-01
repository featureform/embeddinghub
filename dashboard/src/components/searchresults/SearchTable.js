// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import Typography from '@mui/material/Typography';
import { useRouter } from 'next/router';
import { useDataAPI } from '../../hooks/dataAPI';
import React, { useCallback, useEffect, useState } from 'react';
import Resource from '../../api/resources/Resource';
import NoDataMessage from '../resource-list/NoDataMessage';
import TypeIcon from './TypeIcon';
import FilterPanel from './FilterPanel';
import { isMatchingDefault } from '../resource-list/DatasetTable/DatasetTable';
import { MainContainer, GridContainer, StyledDataGrid } from '../resource-list/BaseColumnTable';

export const searchTypeMap = Object.freeze({
  FEATURE: 'Feature',
  FEATURE_VARIANT: 'Feature',
  LABEL: 'Label',
  LABEL_VARIANT: 'Label',
  TRAINING_SET: 'TrainingSet',
  TRAINING_SET_VARIANT: 'TrainingSet',
  SOURCE: 'Source',
  SOURCE_VARIANT: 'Source',
  PROVIDER: 'Provider',
  ENTITY: 'Entity',
  MODEL: 'Model',
  USER: 'User',
});

export const searchColumns = [
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
    field: 'Name',
    headerName: 'Name',
    flex: 1,
    editable: false,
    sortable: false,
    filterable: false,
    hide: false,
    renderCell: function (params) {
      return (
        <>
          <Typography variant='body1' sx={{ marginLeft: 1 }}>
            {params.row?.Name ?? ''}
          </Typography>
        </>
      );
    },
  },
  {
    field: 'Type',
    headerName: 'Type',
    flex: 1,
    editable: false,
    sortable: false,
    filterable: false,
    hide: false,
    renderCell: function (params) {
      const resourceType =
        Resource[searchTypeMap[params.row.Type?.toUpperCase()]];
      return (
        <>
          <TypeIcon iconKey={params.row?.Type} />
          <Typography variant='body1' sx={{ marginLeft: 1 }}>
            {resourceType.typePlural ?? ''}
          </Typography>
        </>
      );
    },
  },
  {
    field: 'Variant',
    headerName: 'Variant',
    flex: 1,
    editable: false,
    sortable: false,
    filterable: false,
    hide: false,
    renderCell: function (params) {
      return (
        <>
          <Typography variant='body1' sx={{ marginLeft: 1 }}>
            {params.row?.Variant ?? ''}
          </Typography>
        </>
      );
    },
  },
  {
    field: 'Tags',
    headerName: 'Tags',
    flex: 1,
    editable: false,
    sortable: false,
    filterable: false,
    hide: false,
    renderCell: function (params) {
      return (
        <div>
          <div style={{ display: 'flex' }}>
            <Typography variant='body2'>
              {sanitizeTags(params.row?.tags)}
            </Typography>
          </div>
        </div>
      );
    },
  },
];

function sanitizeTags(tags = [], maxLength = 25) {
  if (!tags || tags.length === 0) return '';
  //join all the tags together
  const formattedTags = tags.join(', ');

  //if the length is longer than max, chop 3 characters and add the ellipse
  if (formattedTags.length > maxLength) {
    return `${formattedTags.substring(0, maxLength - 3)}...`;
  }

  return formattedTags;
}

const DEFAULT_FILTERS = Object.freeze({
  SearchQuery: '',
  pageSize: 10,
  offset: 0,
});

export const SearchTable = () => {
  const [filters, setFilters] = useState({...DEFAULT_FILTERS});
  const router = useRouter();
  const q = router.query?.q || '';

  const [rows, setRows] = useState([]);
  const [loading, setIsLoading] = useState(false);
  const [totalRowCount, setTotalRowCount] = useState(0);

  const dataAPI = useDataAPI();

  function handleClick(name = '', variant = '', type = '') {
    const resourceType = Resource[searchTypeMap[type?.toUpperCase()]];
    if (resourceType?.hasVariants) {
      const base = resourceType.urlPathResource(name);
      router.push(`${base}?variant=${variant}`);
    } else {
      router.push(resourceType.urlPathResource(name));
    }
  }

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

  const handlePageChange = useCallback(
    (newPage) => {
      const updatedFilters = { ...filters, offset: newPage };
      setFilters(updatedFilters);
    },
    [filters]
  );

  useEffect(() => {
    const updateFilters = { ...filters, SearchQuery: q };
      setFilters(updateFilters);
  }, [q]);

  useEffect(() => {
    const getResources = async () => {
      setIsLoading(true);
      try {
        let resp = await dataAPI.searchResources(filters.SearchQuery);
        if (resp) {
          setRows(resp.length ? resp : []);
          setTotalRowCount(resp.length);
        }
      } catch (error) {
        console.error('Error fetching resources', error);
      } finally {
        setIsLoading(false);
      }
    };
    getResources();
  }, [filters]);
  
  return (
    <>
      <MainContainer>
          <FilterPanel
            filters={filters}
            onCheckBoxChange={checkBoxFilterChange}
          />
        <GridContainer>
          <h3>{'Search Results: ' + filters.SearchQuery}</h3>
          {loading ? (
            <div data-testid='loadingGrid'>
              <StyledDataGrid
                disableVirtualization
                aria-label={'Search Results'}
                autoHeight
                density='compact'
                loading={loading}
                rows={[]}
                rowCount={0}
                columns={searchColumns}
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
              aria-label={'Search Results'}
              rows={rows}
              columns={searchColumns}
              density='compact'
              rowHeight={80}
              hideFooterSelectedRowCount
              disableColumnFilter
              disableColumnMenu
              disableColumnSelector
              paginationMode='client'
              sortModel={[{ field: 'name', sort: 'asc' }]}
              onPageChange={(newPage) => {
                handlePageChange(newPage);
              }}
              components={{
                NoRowsOverlay: () => (
                  <NoDataMessage
                    type={'Search Results'}
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
                if (params?.row?.Name) {
                  handleClick(params.row.Name, params.row.Variant, params.row.Type);
                }
              }}
              getRowId={(row) => row.Name + row.Type}
            />
          )}
        </GridContainer>
      </MainContainer>
    </>
  );
};

export default SearchTable;
