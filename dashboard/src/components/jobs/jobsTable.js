import {
  Button,
  Chip,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from '@mui/material';
import React, { useState } from 'react';

export default function JobsTable({ jobsList = [] }) {
  let dummyJob = {
    name: 'agg1',
    type: 'Source',
    provider: 'Spark',
    resource: 'perc_balance',
    variant: 'v1',
    status: 'Pending',
    lastRuntime: '2024-06-15',
    triggeredBy: 'On Apply',
  };
  jobsList.push(dummyJob, dummyJob, dummyJob, dummyJob, dummyJob);

  const STATUS_ALL = 'ALL';
  const STATUS_ACTIVE = 'ACTIVE';
  const STATUS_COMPLETE = 'COMPLETE';
  const [statusFilter, setStatusFilter] = useState(STATUS_ALL);

  const handleStatusBtnSelect = (statusType = STATUS_ALL) => {
    setStatusFilter(statusType);
  };

  const handleRowSelect = (jobName) => {
    console.log('clicked on jobs row', jobName);
  };

  return (
    <>
      <div style={{ paddingBottom: '25px' }}>
        <Button
          variant='outlined'
          style={
            statusFilter === STATUS_ALL
              ? { color: 'white', background: '#FC195C' }
              : { color: 'black' }
          }
          onClick={() => handleStatusBtnSelect(STATUS_ALL)}
        >
          <Typography
            variant='button'
            style={{ textTransform: 'none', paddingRight: '10px' }}
          >
            All
          </Typography>
          <Chip
            label={56}
            style={
              statusFilter === STATUS_ALL
                ? { color: 'black', background: 'white' }
                : { color: 'white', background: '#FC195C' }
            }
          />
        </Button>
        <Button
          variant='outlined'
          style={
            statusFilter === STATUS_ACTIVE
              ? { color: 'white', background: '#FC195C' }
              : { color: 'black' }
          }
          onClick={() => handleStatusBtnSelect(STATUS_ACTIVE)}
        >
          <Typography
            variant='button'
            style={{ textTransform: 'none', paddingRight: '10px' }}
          >
            Active
          </Typography>
          <Chip
            label={32}
            style={
              statusFilter === STATUS_ACTIVE
                ? { color: 'black', background: 'white' }
                : { color: 'white', background: '#FC195C' }
            }
          />
        </Button>

        <Button
          variant='outlined'
          style={
            statusFilter === STATUS_COMPLETE
              ? { color: 'white', background: '#FC195C' }
              : { color: 'black' }
          }
          onClick={() => handleStatusBtnSelect(STATUS_COMPLETE)}
        >
          <Typography
            variant='button'
            style={{ textTransform: 'none', paddingRight: '10px' }}
          >
            Complete
          </Typography>
          <Chip
            label={24}
            style={
              statusFilter === STATUS_COMPLETE
                ? { color: 'black', background: 'white' }
                : { color: 'white', background: '#FC195C' }
            }
          />
        </Button>
      </div>
      <TableContainer component={Paper}>
        <Table sx={{ minWidth: 300 }} aria-label='Job Runs'>
          <TableHead>
            <TableRow>
              <TableCell>Name</TableCell>
              <TableCell>Type</TableCell>
              <TableCell>Provider</TableCell>
              <TableCell>Resource</TableCell>
              <TableCell>Variant</TableCell>
              <TableCell>Status</TableCell>
              <TableCell>Last Runtime</TableCell>
              <TableCell>Triggered By</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {jobsList?.map((job, index) => {
              return (
                <TableRow
                  key={index}
                  onClick={() => handleRowSelect(job.name)}
                  style={{ cursor: 'pointer' }}
                  hover
                >
                  <TableCell>{job.name}</TableCell>
                  <TableCell>{job.type}</TableCell>
                  <TableCell>{job.provider}</TableCell>
                  <TableCell>{job.resource}</TableCell>
                  <TableCell>{job.variant}</TableCell>
                  <TableCell>{job.status}</TableCell>
                  <TableCell>{job.lastRuntime}</TableCell>
                  <TableCell>{job.triggeredBy}</TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      </TableContainer>
    </>
  );
}
