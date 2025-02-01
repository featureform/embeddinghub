// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import {
  AppBar,
  Box,
  Button,
  Chip,
  Container,
  FormControl,
  Grid,
  Icon,
  MenuItem,
  Select,
  Tab,
  Tabs,
  ThemeProvider,
  Typography,
} from '@mui/material';
import { styled } from '@mui/system';
import { DataGrid } from '@mui/x-data-grid';
import { useRouter } from 'next/router';
import PropTypes from 'prop-types';
import React, { useEffect, useState } from 'react';
import { PrismAsyncLight as SyntaxHighlighter } from 'react-syntax-highlighter';
import json from 'react-syntax-highlighter/dist/cjs/languages/prism/json';
import python from 'react-syntax-highlighter/dist/cjs/languages/prism/python';
import sql from 'react-syntax-highlighter/dist/cjs/languages/prism/sql';
import { okaidia } from 'react-syntax-highlighter/dist/cjs/styles/prism';
import { format } from 'sql-formatter';
import Resource from '../../api/resources/Resource';
import theme from '../../styles/theme';
import SourceDialog from '../dialog/SourceDialog';
import AttributeBox from './elements/AttributeBox';
import MetricsDropdown from './elements/MetricsDropdown';
import StatsDropdown from './elements/StatsDropdown';
import TagBox from './elements/TagBox';
import VariantControl from './elements/VariantControl';
import ErrorModal, { ERROR_MSG_MAX } from './ErrorModal';
import StatusChip from './statusChip';

SyntaxHighlighter.registerLanguage('python', python);
SyntaxHighlighter.registerLanguage('sql', sql);
SyntaxHighlighter.registerLanguage('json', json);

export function getFormattedSQL(sqlString = '') {
  let stringResult = sqlString;
  try {
    stringResult = format(sqlString.replace('{{ ', '"').replace(' }}', '"'), {
      language: 'sql',
    });
  } catch {
    console.error('There was an error formatting the sql string');
    console.error(stringResult);
    stringResult = sqlString;
  }
  return stringResult;
}

function TabPanel(props) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role='tabpanel'
      hidden={value !== index}
      id={`simple-tabpanel-${index}`}
      aria-labelledby={`simple-tab-${index}`}
      {...other}
    >
      {value === index && <Box p={3}>{children}</Box>}
    </div>
  );
}

TabPanel.propTypes = {
  children: PropTypes.node,
  index: PropTypes.any.isRequired,
  value: PropTypes.any.isRequired,
};

const StyledContainer = styled(Container)(({ theme }) => ({
  flexGrow: 1,
  padding: theme.spacing(0),
  backgroundColor: theme.palette.background.paper,
  marginTop: theme.spacing(2),
  border: `2px solid ${theme.palette.border.main}`,
  borderRadius: '16px',
}));

const MetadataContainer = styled(Grid)(({ theme }) => ({
  padding: theme.spacing(1),
  borderRadius: '16px',
  border: `1px solid ${theme.palette.border.main}`,
}));

const TagBoxContainer = styled(Grid)(({ theme }) => ({
  padding: theme.spacing(1),
  borderRadius: '16px',
  border: `1px solid ${theme.palette.border.main}`,
}));

const ResourceItem = styled('div')(({ theme }) => ({
  paddingBottom: theme.spacing(1),
  display: 'flex',
  flexDirection: 'row',
}));

const ItemBox = styled(Box)(({ theme }) => ({
  display: 'flex',
  flexDirection: 'row',
  marginRight: theme.spacing(1),
}));

const ItemTypography = styled(Typography)(({ theme }) => ({
  marginRight: theme.spacing(1),
}));

const SyntaxContainer = styled(SyntaxHighlighter)(({ theme }) => ({
  width: '200px + 50%',
  minWidth: '200px',
  paddingLeft: theme.spacing(2),
}));

const CustomAppBar = styled(AppBar)(() => ({
  background: 'transparent',
  boxShadow: 'none',
  color: 'black',
}));

const ConfigContainer = styled('div')(({ theme }) => ({
  flexGrow: 1,
  paddingLeft: theme.spacing(2),
  marginTop: theme.spacing(2),
  marginLeft: theme.spacing(2),
}));

const ResourcesTopRow = styled('div')(() => ({
  display: 'flex',
  justifyContent: 'space-between',
  marginBottom: 10,
  marginTop: 10,
}));

const StyledDataGrid = styled(DataGrid)(() => ({
  '& .MuiDataGrid-cell:focus': {
    outline: 'none',
  },
  '& .MuiDataGrid-columnHeader:focus': {
    outline: 'none',
  },
  '& .MuiDataGrid-colCellWrapper': {
    display: 'none',
  },
  marginBottom: '1.5em',
  cursor: 'pointer',
  '& .MuiDataGrid-columnHeaderTitle': {
    fontWeight: 'bold',
  },
}));

function a11yProps(index) {
  return {
    id: `simple-tab-${index}`,
    'aria-controls': `simple-tabpanel-${index}`,
  };
}

export const convertInputToDate = (timestamp_string_or_mil = '') => {
  const input = timestamp_string_or_mil;

  if (isNaN(input)) {
    return generateDate(input);
  } else {
    const createdTimeInSeconds = parseFloat(input); //resources.py returns seconds, not milliseconds
    const createdTimeMilliSeconds = Math.round(createdTimeInSeconds * 1000);
    return generateDate(createdTimeMilliSeconds);
  }

  function generateDate(str) {
    return new Date(str).toLocaleString('en-US', {
      timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone,
    });
  }
};

const EntityPageView = ({
  api,
  entity,
  setVariant,
  activeVariants,
  queryVariant = '',
}) => {
  let resources = entity.resources;
  let resourceType = Resource[resources.type];
  let type = resourceType.type;
  const router = useRouter();
  // todox: setting to false. decide in the future what to do with prometheus charts
  const showMetrics = process.env.NODE_ENV == 'production' ? false : false;
  const showStats = false;
  /*eslint-disable no-constant-condition*/
  const dataTabDisplacement = (1 ? showMetrics : 0) + (1 ? showStats : 0);
  const statsTabDisplacement = showMetrics ? 1 : 0;
  const name = resources['name'];
  const icon = resourceType.materialIcon;
  const foundQueryVariant = resources['all-variants']?.find(
    (vr) => vr === queryVariant
  );
  const [variant, setLocalVariant] = useState(
    activeVariants[entity.resources.type][name] ||
      foundQueryVariant ||
      resources['default-variant']
  );

  useEffect(() => {
    const foundQueryVariant = resources['all-variants']?.find(
      (vr) => vr === queryVariant
    );
    const activeVariant = activeVariants[entity.resources.type][name];
    if (activeVariant) {
      setLocalVariant(activeVariant);
    } else if (foundQueryVariant) {
      setLocalVariant(foundQueryVariant);
    } else {
      setVariant(entity.resources.type, name, resources['default-variant']);
    }
  }, [activeVariants]);

  let resource;
  if (resourceType.hasVariants) {
    resource = resources.variants[variant];
  } else {
    resource = resources;
  }
  let metadata = {};
  let resourcesData = {};

  Object.keys(resource).forEach((key) => {
    if (Resource.pathToType[key]) {
      resourcesData[Resource.pathToType[key]] = resource[key];
    } else {
      metadata[key] = resource[key];
    }
  });

  const numValidKeys = (dict) => {
    let validKeys = 0;
    Object.keys(dict).forEach(function (key) {
      if (metadata['specifications'][key] !== '') {
        validKeys++;
      }
    });
    return validKeys;
  };

  let allVariants = resources['all-variants'];

  const [value, setValue] = useState(0);

  const handleVariantChange = (variantParam = '') => {
    if (variantParam) {
      const base = Resource[type].urlPathResource(name);
      setVariant(type, name, variantParam);
      router.replace(`${base}?variant=${variantParam}`, null, {
        shallow: true,
      });
    }
  };

  const handleChange = (_, newValue) => {
    setValue(newValue);
  };

  const linkToEntityPage = () => {
    router.push(`/entities/${metadata['entity']}`);
  };

  const linkToLineage = (nameVariant = { Name: '', Variant: '' }) => {
    if (nameVariant.Name && nameVariant.Variant) {
      setVariant('Source', nameVariant.Name, nameVariant.Variant);
      router.push(
        `/sources/${nameVariant.Name}?variant=${nameVariant.Variant}`
      );
    } else {
      console.warn('linkToLineage() Namevariant properties are missing');
    }
  };

  const linkToTaskRuns = (name = '', variant = '') => {
    if (name && variant) {
      router.push(`/tasks?name=${name}&variant=${variant}`);
    } else {
      console.warn(
        `linkToTaskRuns() name(${name}) and variant(${variant}) properties are missing`
      );
    }
  };

  const linkToLabel = () => {
    router.push(`/labels/${metadata['label'].Name}`);
  };

  const linkToUserPage = () => {
    router.push(`/users/${metadata['owner']}`);
  };

  const linkToProviderPage = () => {
    router.push(`/providers/${metadata['provider']}`);
  };

  let resourceHeading = `${resources.type}`
  if (resourceHeading === 'Source') {
    resourceHeading = 'Dataset'
  }

  const metadataInputSet = Array.from(new Set(metadata['inputs']));

  return true || (!resources.loading && !resources.failed && resources.data) ? (
    <StyledContainer maxWidth={false}>
      <Box>
        <Grid container justifyContent='flex-start'>
          <Grid item xs={false}></Grid>
          <Grid item xs={12} lg={12}>
            <ResourcesTopRow>
              <ItemBox>
                <Icon sx={{ marginRight: 1, marginTop: 1 }}>{icon}</Icon>
                <Box>
                  <Typography variant='h4' component='h4'>
                    <span>
                      {`${resourceHeading}: `}
                      <strong>{resources.name}</strong>
                    </span>
                  </Typography>
                  {metadata['created'] && (
                    <Typography variant='subtitle1'>
                      Created: {convertInputToDate(metadata['created'])}
                    </Typography>
                  )}
                </Box>
              </ItemBox>
              {allVariants && (
                <Box>
                  <VariantControl
                    variant={variant}
                    variantListProp={allVariants}
                    resources={resources}
                    handleVariantChange={handleVariantChange}
                    type={type}
                    name={name}
                  />
                </Box>
              )}
            </ResourcesTopRow>
          </Grid>
        </Grid>
        {Object.keys(metadata).length > 0 && metadata['status'] != 'NO_STATUS' && (
          <Box>
            <Grid container spacing={0}>
              <MetadataContainer item xs={8}>
                {metadata['description'] && (
                  <ResourceItem>
                    <ItemTypography variant='body1'>
                      <strong>Description:</strong> {metadata['description']}
                    </ItemTypography>
                  </ResourceItem>
                )}

                {metadata['owner'] && (
                  <ResourceItem>
                    <ItemBox>
                      <ItemTypography sx={{ marginRight: 1 }} variant='body1'>
                        <strong>Owner:</strong>{' '}
                      </ItemTypography>
                      <Chip
                        variant='outlined'
                        size='small'
                        onClick={linkToUserPage}
                        label={metadata['owner']}
                      ></Chip>
                    </ItemBox>
                  </ResourceItem>
                )}

                {metadata['provider'] && (
                  <ResourceItem>
                    <ItemBox>
                      <ItemTypography variant='body1'>
                        <strong>Provider:</strong>{' '}
                      </ItemTypography>
                      <Chip
                        variant='outlined'
                        size='small'
                        onClick={linkToProviderPage}
                        label={metadata['provider']}
                      ></Chip>
                    </ItemBox>
                  </ResourceItem>
                )}

                {metadata['dimensions'] && (
                  <ResourceItem>
                    <ItemTypography variant='body1'>
                      <strong>Dimensions:</strong> {metadata['dimensions']}
                    </ItemTypography>
                  </ResourceItem>
                )}
                {metadata['data-type'] && (
                  <ResourceItem>
                    <ItemTypography variant='body1'>
                      <strong>Data Type:</strong> {metadata['data-type']}
                    </ItemTypography>
                  </ResourceItem>
                )}
                {metadata['joined'] && (
                  <ResourceItem>
                    <ItemTypography variant='body1'>
                      <strong>Joined:</strong>{' '}
                      {convertInputToDate(metadata['joined'])}
                    </ItemTypography>
                  </ResourceItem>
                )}
                {metadata['software'] && (
                  <ResourceItem>
                    <ItemTypography variant='body1'>
                      <strong>Software:</strong> {metadata['software']}
                    </ItemTypography>
                  </ResourceItem>
                )}
                {metadata['label'] && (
                  <ResourceItem>
                    <ItemBox>
                      <ItemTypography variant='body1'>
                        <strong>Label: </strong>{' '}
                      </ItemTypography>
                      <Chip
                        variant='outlined'
                        size='small'
                        onClick={linkToLabel}
                        label={metadata['label'].Name}
                      ></Chip>
                    </ItemBox>
                  </ResourceItem>
                )}
                {metadata['provider-type'] && (
                  <ResourceItem>
                    <ItemTypography variant='body1'>
                      <strong>Provider Type:</strong>{' '}
                      {metadata['provider-type']}
                    </ItemTypography>
                  </ResourceItem>
                )}
                {metadata['team'] && (
                  <ResourceItem>
                    <ItemTypography variant='body1'>
                      <strong>Team:</strong> {metadata['team']}
                    </ItemTypography>
                  </ResourceItem>
                )}
                {metadata['status'] && metadata['status'] !== 'NO_STATUS' && (
                  <ResourceItem>
                    <ItemTypography
                      variant='body1'
                      style={{ marginRight: 10, marginTop: 4 }}
                    >
                      <strong>Status:</strong>{' '}
                    </ItemTypography>
                    <StatusChip status={metadata['status']} />
                  </ResourceItem>
                )}
                {metadata['error'] && metadata['error'] !== '' && (
                  <>
                    <ResourceItem>
                      <ItemTypography style={{ width: 150 }}>
                        <strong>Error Message:</strong>
                      </ItemTypography>
                      {metadata['error'].length > ERROR_MSG_MAX ? (
                        <>
                          <ErrorModal
                            buttonTxt='See More'
                            errorTxt={metadata['error']}
                          />
                        </>
                      ) : (
                        <>
                          <ItemTypography
                            variant='body1'
                            sx={{
                              whiteSpace: 'pre-line',
                              color: 'red',
                            }}
                          >
                            {metadata['error']}
                          </ItemTypography>
                        </>
                      )}
                    </ResourceItem>
                  </>
                )}
                {metadata['source-type'] && (
                  <ResourceItem>
                    <ItemTypography variant='body1'>
                      <strong>Dataset Type:</strong> {metadata['source-type']}
                    </ItemTypography>
                  </ResourceItem>
                )}
                {metadata['specifications'] &&
                  numValidKeys(metadata['specifications']) > 0 && (
                    <ResourceItem>
                      <ItemTypography variant='body1'>
                        <strong>Specifications:</strong>
                      </ItemTypography>
                      <ItemTypography variant='body1' component={'h2'}>
                        {Object.keys(metadata['specifications']).map(
                          (k, index) =>
                            metadata['specifications'][k] !== '' && (
                              <Box key={index} style={{ marginLeft: 16 }}>
                                <strong>{k}: </strong>{' '}
                                {metadata['specifications'][k]}
                              </Box>
                            )
                        )}
                      </ItemTypography>
                    </ResourceItem>
                  )}

                {metadata['serialized-config'] && (
                  <ResourceItem>
                    <ItemTypography variant='body1'>
                      <strong>Serialized Config:</strong>{' '}
                      {metadata['serialized-config']}
                    </ItemTypography>
                  </ResourceItem>
                )}

                {metadata['source']?.Name && (
                  <ResourceItem>
                    <ItemBox>
                      <ItemTypography variant='body1'>
                        <strong>Dataset: </strong>{' '}
                      </ItemTypography>
                      <Chip
                        variant='outlined'
                        size='small'
                        onClick={() =>
                          linkToLineage({
                            Name: metadata['source']?.Name,
                            Variant: metadata['source']?.Variant,
                          })
                        }
                        label={`${metadata['source'].Name} (${metadata['source'].Variant})`}
                      ></Chip>
                    </ItemBox>
                  </ResourceItem>
                )}

                {metadata['entity'] && (
                  <ResourceItem>
                    <ItemBox>
                      <ItemTypography variant='body1'>
                        <strong>Entity:</strong>{' '}
                      </ItemTypography>
                      <Chip
                        variant='outlined'
                        size='small'
                        onClick={linkToEntityPage}
                        label={metadata['entity']}
                      ></Chip>
                    </ItemBox>
                  </ResourceItem>
                )}

                {metadata['location'] && !metadata['is-on-demand'] && (
                  <ResourceItem>
                    <ItemBox>
                      <ItemTypography variant='body1'>
                        <strong>Columns:</strong>{' '}
                      </ItemTypography>
                      <ItemTypography variant='body2' style={{ marginTop: 2 }}>
                        &nbsp;<strong>Entity:</strong>{' '}
                        {metadata['location'].Entity}
                        &nbsp;<strong>Value:</strong>{' '}
                        {metadata['location'].Value}
                        {metadata['location'].TS ? (
                          <>
                            &nbsp;<strong>Timestamp:</strong>{' '}
                            {metadata['location'].TS}
                          </>
                        ) : (
                          ''
                        )}
                      </ItemTypography>
                    </ItemBox>
                  </ResourceItem>
                )}

                {metadata['location'] && metadata['is-on-demand'] && (
                  <ResourceItem>
                    <ItemBox>
                      <ItemTypography variant='body1'>
                        <strong>Feature Variant Type:</strong>{' '}
                      </ItemTypography>
                      <Chip
                        variant='outlined'
                        size='small'
                        onClick={() => {}}
                        label={'On-Demand'}
                      ></Chip>
                    </ItemBox>
                  </ResourceItem>
                )}

                {metadataInputSet?.length ? (
                  <ResourceItem>
                    <ItemBox>
                      <ItemTypography variant='body1'>
                        <strong>Datasets:</strong>
                      </ItemTypography>
                      {metadataInputSet.map((nv, index) => (
                        <Chip
                          key={index}
                          variant='outlined'
                          size='small'
                          onClick={() => linkToLineage(nv)}
                          label={`${nv.Name} (${nv.Variant})`}
                        ></Chip>
                      ))}
                    </ItemBox>
                  </ResourceItem>
                ) : null}

                {metadata['definition'] ? (
                  <div>
                    {(() => {
                      if (metadata['source-type'] === 'SQL Transformation') {
                        return (
                          <SyntaxContainer language={'sql'} style={okaidia}>
                            {getFormattedSQL(metadata['definition'])}
                          </SyntaxContainer>
                        );
                      } else if (
                        ['Dataframe Transformation'].includes(
                          metadata['source-type']
                        ) ||
                        metadata['is-on-demand'] === true
                      ) {
                        return (
                          <SyntaxContainer language={'python'} style={okaidia}>
                            {metadata['definition']}
                          </SyntaxContainer>
                        );
                      } else {
                        return (
                          <ItemTypography variant='h7'>
                            <strong>{metadata['definition']}</strong>
                          </ItemTypography>
                        );
                      }
                    })()}

                    {(() => {
                      if (
                        type === 'Source' &&
                        metadata['status']?.toUpperCase() !== 'FAILED' &&
                        metadata['status']?.toUpperCase() !== 'PENDING'
                      ) {
                        return (
                          <div style={{ paddingBottom: 5 }}>
                            <SourceDialog
                              api={api}
                              sourceName={name}
                              sourceVariant={variant}
                            />
                          </div>
                        );
                      }
                    })()}
                  </div>
                ) : null}
                {metadata['is-on-demand'] !== true &&
                  !['provider', 'entity', 'user', 'model'].includes(
                    metadata['type']?.toLowerCase()
                  ) && (
                    <Button
                      data-testid='taskRunBtnId'
                      variant='outlined'
                      onClick={() =>
                        linkToTaskRuns(metadata['name'], metadata['variant'])
                      }
                    >
                      Task Runs
                    </Button>
                  )}
              </MetadataContainer>

              <TagBoxContainer item xs={4}>
                <TagBox
                  resourceName={name}
                  variant={variant}
                  type={resourceType._urlPath}
                  tags={metadata['tags']}
                  title={'Tags'}
                />
              </TagBoxContainer>
              {Object.keys(metadata['properties'] || {}).length > 0 && (
                <Grid item xs>
                  {
                    <AttributeBox
                      attributes={Object.entries(metadata['properties']).map(
                        ([k, v]) => `${k}:${v}`
                      )}
                      title={'Properties'}
                    />
                  }
                </Grid>
              )}
            </Grid>
          </Box>
        )}
        {metadata['config'] && (
          <ResourceItem>
            <ConfigContainer>
              <Typography variant='body1'>
                <strong>Config:</strong>
              </Typography>
              <SyntaxContainer language={metadata['language']} style={okaidia}>
                {metadata['config']}
              </SyntaxContainer>
            </ConfigContainer>
          </ResourceItem>
        )}
      </Box>

      <Box>
        <CustomAppBar position='static'>
          <Tabs
            value={value}
            onChange={handleChange}
            aria-label='simple tabs example'
          >
            {showMetrics && <Tab label={'metrics'} {...a11yProps(0)} />}
            {showStats && (
              <Tab label={'stats'} {...a11yProps(statsTabDisplacement)} />
            )}
            {Object.keys(resourcesData).map((key, i) => (
              <Tab
                key={i}
                label={Resource[key].typePlural}
                {...a11yProps(i + dataTabDisplacement)}
              />
            ))}
          </Tabs>
        </CustomAppBar>
        {showMetrics && (
          <TabPanel value={value} key={'metrics'} index={0}>
            <MetricsDropdown type={type} name={name} variant={variant} />
          </TabPanel>
        )}
        {showStats && (
          <TabPanel value={value} key={'stats'} index={statsTabDisplacement}>
            <StatsDropdown type={type} name={name} />
          </TabPanel>
        )}

        {Object.keys(resourcesData).map((resourceType, i) => (
          <TabPanel
            value={value}
            key={resourceType}
            index={i + dataTabDisplacement}
          >
            <ThemeProvider theme={theme}>
              <Box style={{ height: 400, width: '100%' }}>
                <StyledDataGrid
                  columns={[
                    {
                      field: 'name',
                      headerName: 'Name',
                      flex: 1,
                      sortable: false,
                    },
                    {
                      field: 'variant',
                      headerName: 'Variant',
                      flex: 1,
                      sortable: false,
                    },
                  ]}
                  rows={Object.entries(resourcesData[resourceType]).map(
                    (resourceEntry, index) => {
                      const resourceName = resourceEntry[0];
                      const resourceVariants = resourceEntry[1];
                      let rowData = { id: index, name: resourceName };
                      if (resourceVariants.length) {
                        rowData['variant'] = resourceVariants[0].variant;
                      } else {
                        rowData['variant'] = '';
                      }
                      rowData['variants'] = Object.values(resourceVariants);
                      return rowData;
                    }
                  )}
                  disableColumnFilter
                  disableColumnMenu
                  disableColumnSelector
                  pageSize={5}
                  autoHeight
                  rowsPerPageOptions={[5]}
                  onRowClick={(params) => {
                    const resource = Resource[resourceType];
                    const base = resource.urlPathResource(params.row.name);
                    if (resource?.hasVariants && params.row.variant) {
                      setVariant(
                        resource.type,
                        params.row.name,
                        params.row.variant
                      );
                      router.push(`${base}?variant=${params.row.variant}`);
                    } else {
                      router.push(base);
                    }
                  }}
                />
              </Box>
            </ThemeProvider>
          </TabPanel>
        ))}
      </Box>
    </StyledContainer>
  ) : (
    <div></div>
  );
};

export const TagList = ({ activeTags = {}, tags = [] }) => (
  <Grid container direction='row'>
    {tags.map((tag) => (
      <Chip
        key={tag}
        color={activeTags[tag] ? 'secondary' : 'default'}
        onClick={() => null}
        variant='outlined'
        label={tag}
      />
    ))}
  </Grid>
);

export const VariantSelector = ({ variants = [''] }) => (
  <FormControl>
    <Select value={variants[0]}>
      {variants.map((variant) => (
        <MenuItem
          key={variant}
          value={variant}
          onClick={(event) => event.stopPropagation()}
        >
          {variant}
        </MenuItem>
      ))}
    </Select>
  </FormControl>
);

export default EntityPageView;
