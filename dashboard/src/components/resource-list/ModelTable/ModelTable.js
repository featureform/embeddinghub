import { Typography } from '@mui/material';
import { useRouter } from 'next/router';
import React, { useCallback, useEffect, useState } from 'react';
import { useDataAPI } from '../../../hooks/dataAPI';
import { isMatchingDefault } from '../DatasetTable/DatasetTable';
import NoDataMessage from '../NoDataMessage';
import FilterPanel from './FilterPanel';
import BaseFilterPanel from '../BaseFilterPanel';
import { MainContainer, GridContainer, StyledDataGrid } from '../BaseColumnTable';

export const model_columns = [
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
    headerName: 'Name',
    flex: 1,
    editable: false,
    sortable: false,
    filterable: false,
    hide: false,
    renderCell: function ({ row }) {
      return (
        <Typography variant='body2' sx={{ marginLeft: 1 }}>
          <strong>{row?.name}</strong>
        </Typography>
      );
    },
  },
  {
    field: 'tags',
    headerName: 'Tags',
    flex: 1,
    editable: false,
    sortable: false,
    filterable: false,
    hide: false,
    renderCell: function ({ row }) {
      return (
          <div style={{ display: 'flex' }}>
            <Typography variant='body2'>
              {sanitizeTags(row?.tags)}
            </Typography>
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
  SearchTxt: '',
  Tags: [],
  pageSize: 10,
  offset: 0,
});

const CHECK_BOX_LIMIT = 8;

export const ModelTable = () => {
  const [tags, setTags] = useState([]);
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
        let resp = await dataAPI.getModels(filters);
        if (resp) {
          setRows(resp.data?.length ? resp.data : []);
          setTotalRowCount(resp.count);
        }
      } catch (error) {
        console.error('Error fetching models', error);
      } finally {
        setIsLoading(false);
      }
    };
    getResources();
  }, [filters]);

  useEffect(() => {
    const getCheckBoxLists = async () => {
      let allTags = await dataAPI.getTypeTags('models');
      if (allTags) {
        setTags(allTags.slice(0, CHECK_BOX_LIMIT));
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

  const redirect = (name = '') => {
    if (name) {
      router.push(`/models/${name}`);
    }
  };

  return (
    <>
      <MainContainer>
      <BaseFilterPanel aria-label="Model filters" onTextFieldEnter={onTextFieldEnter}>
        <FilterPanel
          aria-label="Model tag filters"
          filters={filters}
          tags={tags}
          onCheckBoxChange={checkBoxFilterChange}
        />
      </BaseFilterPanel>
        <GridContainer>
          <h3>{'Models'}</h3>
          {loading ? (
            <div data-testid='loadingGrid'>
              <StyledDataGrid
                disableVirtualization
                aria-label={'Models'}
                autoHeight
                density='compact'
                loading={loading}
                rows={[]}
                rowCount={0}
                columns={model_columns}
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
              aria-label={'Models'}
              rows={rows}
              columns={model_columns}
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
                    type={'Model'}
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
                  redirect(params.row.name);
                }
              }}
              getRowId={(row) => row.name}
            />
          )}
        </GridContainer>
      </MainContainer>
    </>
  );
};

export default ModelTable;