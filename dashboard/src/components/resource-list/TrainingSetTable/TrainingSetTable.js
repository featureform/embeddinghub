import { Typography } from '@mui/material';
import { styled } from '@mui/system';
import { useRouter } from 'next/router';
import React, { useCallback, useEffect, useState } from 'react';
import { providerLogoMap } from '../../../api/resources';
import { useDataAPI } from '../../../hooks/dataAPI';
import { isMatchingDefault } from '../DatasetTable/DatasetTable';
import { ConnectionSvg } from '../icons/Connections';
import { UserBubbleSvg } from '../icons/Owner';
import NoDataMessage from '../NoDataMessage';
import FilterPanel from './FilterPanel';
import BaseFilterPanel from '../BaseFilterPanel';
import { MainContainer, GridContainer, StyledDataGrid } from '../BaseColumnTable';

const ProviderImage = styled('img')({
  width: '2.5em',
  height: '2em',
});

export const training_set_columns = [
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
    field: 'provider',
    headerName: 'Provider',
    flex: 1,
    editable: false,
    sortable: false,
    filterable: false,
    hide: false,
    renderCell: function ({ row }) {
      const providerTxt = row?.provider || row?.providerType || 'Local';
      const provider =
        providerLogoMap[row?.providerType?.toUpperCase()] ??
        providerLogoMap['LOCAL_ONLINE'];
      return (
        <>
          <ProviderImage alt={row?.providerType} src={provider} />
          <Typography variant='body2' sx={{ marginLeft: 1 }}>
            {providerTxt}
          </Typography>
        </>
      );
    },
  },
  {
    field: 'labelName',
    headerName: 'Label',
    flex: 1,
    editable: false,
    sortable: false,
    filterable: false,
    renderCell: function ({ row }) {
      const DEFAULT_LABEL = 'N/A';
      const labelName = row?.label?.Name || DEFAULT_LABEL;
      return (
        <Typography variant='body2' sx={{ marginLeft: 1 }} aria-label={`Label: ${labelName}`}>
          {labelName}
        </Typography>
      );
    },
  },
  {
    field: 'status',
    headerName: 'Status',
    flex: 0,
    width: 300,
    editable: false,
    sortable: false,
    filterable: false,
    renderCell: function ({ row }) {
      const readyFill = '#6DDE6A';
      let result = '#DA1E28';
      if (row?.status && row?.status === 'READY') {
        result = readyFill;
      }
      return (
        <div>
          <div style={{ display: 'flex' }}>
            <ConnectionSvg fill={result} height='20' width='20' />
            <Typography variant='body2' sx={{ marginLeft: 1 }}>
              {row?.status}
            </Typography>
          </div>
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
];

const DEFAULT_FILTERS = Object.freeze({
  SearchTxt: '',
  Owners: [],
  Statuses: [],
  Labels: [],
  Tags: [],
  Providers: [],
  pageSize: 10,
  offset: 0,
});

const CHECK_BOX_LIMIT = 8;

export const TrainingSetTable = () => {
  const [tags, setTags] = useState([]);
  const [owners, setOwners] = useState([]);
  const [labels, setLabels] = useState([]);
  const [providers, setProviders] = useState([]);
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
        let resp = await dataAPI.getTrainingSetVariants(filters);
        if (resp) {
          setRows(resp.data?.length ? resp.data : []);
          setTotalRowCount(resp.count);
        }
      } catch (error) {
        console.error('Error fetching training sets', error);
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

      const [allTags, allOwners, allLabels, allProviders] = await Promise.all([
        dataAPI.getTypeTags('training-sets'),
        dataAPI.getTypeOwners('training-sets'),
        dataAPI.getLabelVariants(),
        dataAPI.getProviders()
      ]);

      if (allTags) {
        setTags(allTags.slice(0, CHECK_BOX_LIMIT));
      }

      if (allOwners) {
        setOwners(allOwners.slice(0, CHECK_BOX_LIMIT));
      }
      
      if (allLabels) {
        setLabels(sliceAndMap(allLabels.data));
      }

      if (allProviders) {
        setProviders(sliceAndMap(allProviders.data));
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
      router.push(`/training-sets/${name}?variant=${variant}`);
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
          labels={labels}
          providers={providers}
          onCheckBoxChange={checkBoxFilterChange}
        />
      </BaseFilterPanel>
        <GridContainer>
          <h3>{'Training Sets'}</h3>
          {loading ? (
            <div data-testid='loadingGrid'>
              <StyledDataGrid
                disableVirtualization
                aria-label={'Training Sets'}
                autoHeight
                density='compact'
                loading={loading}
                rows={[]}
                rowCount={0}
                columns={training_set_columns}
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
              aria-label={'Training Sets'}
              rows={rows}
              columns={training_set_columns}
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
                    type={'Training Set'}
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
              getRowId={(row) => row.name + row.variant}
            />
          )}
        </GridContainer>
      </MainContainer>
    </>
  );
};

export default TrainingSetTable;
