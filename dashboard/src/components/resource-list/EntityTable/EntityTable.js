import { Typography } from '@mui/material';
import { useRouter } from 'next/router';
import React, { useCallback, useEffect, useState } from 'react';
import { useDataAPI } from '../../../hooks/dataAPI';
import { isMatchingDefault } from '../DatasetTable/DatasetTable';
import NoDataMessage from '../NoDataMessage';
import FilterPanel from './FilterPanel';
import BaseFilterPanel from '../BaseFilterPanel';
import { MainContainer, GridContainer, StyledDataGrid } from '../BaseColumnTable';

export const entity_columns = [
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
    renderCell: function (params) {
      return (
        <>
          <Typography variant='body1' sx={{ marginLeft: 1 }}>
            <strong>{params.row?.name}</strong>
          </Typography>
        </>
      );
    },
  },
  {
    field: 'featureCount',
    headerName: 'Features',
    flex: 1,
    editable: false,
    sortable: false,
    filterable: false,
    renderCell: function (params) {
      const featureCount = params.row?.feature?.length ?? 0;
      return (
        <Typography variant='body1' sx={{ marginLeft: 1 }}>
          {featureCount}
        </Typography>
      );
    },
  },
  {
    field: 'labelCount',
    headerName: 'Labels',
    flex: 1,
    editable: false,
    sortable: false,
    filterable: false,
    renderCell: function (params) {
      const labelCount = params.row?.labels ? params.row.labels.length : 0;
      return (
        <Typography variant='body1' sx={{ marginLeft: 1 }}>
          {labelCount}
        </Typography>
      );
    },
  },
];

const DEFAULT_FILTERS = Object.freeze({
  SearchTxt: '',
  Statuses: [],
  Tags: [],
  Labels: [],
  Features: [],
  pageSize: 10,
  offset: 0,
});

const CHECK_BOX_LIMIT = 8;

export const EntityTable = () => {
  const [tags, setTags] = useState([]);
  const [labels, setLabels] = useState([]);
  const [features, setFeatures] = useState([]);
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
        let resp = await dataAPI.getEntities(filters);
        if (resp) {
          setRows(resp.data?.length ? resp.data : []);
          setTotalRowCount(resp.count);
        }
      } catch (error) {
        console.error('Error fetching entities', error);
      } finally {
      setIsLoading(false);
      }
    };
    getResources();
  }, [filters]);

  useEffect(() => {
    const getCheckBoxLists = async () => {
      const sliceAndMap = (data) => 
        data.slice(0, CHECK_BOX_LIMIT).map(item => item.name);

      const [allTags, allLabels, allFeatures] = await Promise.all([
        dataAPI.getTypeTags('entities'),
        dataAPI.getLabelVariants(),
        dataAPI.getFeatureVariants()
      ]);

      if (allTags) {
        setTags(allTags.slice(0, CHECK_BOX_LIMIT));
      }

      if (allLabels) {
        setLabels(sliceAndMap(allLabels.data));
      }

      if (allFeatures) {
        setFeatures(sliceAndMap(allFeatures.data));
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
    router.push(`/entities/${name}`);
  };

  return (
    <>
      <MainContainer>
        <BaseFilterPanel onTextFieldEnter={onTextFieldEnter}>
          <FilterPanel
            filters={filters}
            tags={tags}
            labels={labels}
            features={features}
            onCheckBoxChange={checkBoxFilterChange}
          />
        </BaseFilterPanel>
        <GridContainer>
          <h3>{'Entities'}</h3>
          {loading ? (
            <div data-testid='loadingGrid'>
              <StyledDataGrid
                disableVirtualization
                aria-label={'Entities'}
                autoHeight
                density='compact'
                loading={loading}
                rows={[]}
                rowCount={0}
                columns={entity_columns}
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
              aria-label={'Entity'}
              rows={rows}
              columns={entity_columns}
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
                    type={'Entity'}
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
              getRowId={(row) => row.name}
            />
          )}
        </GridContainer>
      </MainContainer>
    </>
  );
};

export default EntityTable;
