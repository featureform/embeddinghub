import { Box, CircularProgress } from '@mui/material';
import Button from '@mui/material/Button';
import Dialog from '@mui/material/Dialog';
import DialogActions from '@mui/material/DialogActions';
import DialogContent from '@mui/material/DialogContent';
import DialogTitle from '@mui/material/DialogTitle';
import * as React from 'react';
import SourceDialogTable from './SourceDialogTable';

export default function SourceDialog({
  api,
  sourceName = '',
  sourceVariant = '',
}) {
  const [open, setOpen] = React.useState(false);
  const [columns, setColumns] = React.useState([]);
  const [rowList, setRowList] = React.useState([]);
  const [error, setError] = React.useState('');
  const [isLoading, setIsLoading] = React.useState(true);

  React.useEffect(async () => {
    if (sourceName && sourceVariant && open) {
      setIsLoading(true);
      let response = await api.fetchSourceModalData(sourceName, sourceVariant);
      if (response?.columns && response?.rows) {
        setColumns(response.columns);
        setRowList(response.rows);
      } else {
        setError(response);
      }
      setIsLoading(false);
    }
  }, [sourceName, sourceVariant, open]);

  const handleClickOpen = () => {
    setOpen(true);
  };

  const handleClose = () => {
    setOpen(false);
  };

  return (
    <div>
      <Button
        data-testid='sourceTableOpenId'
        variant='outlined'
        onClick={handleClickOpen}
      >
        Open Table Source
      </Button>
      <Dialog
        fullWidth={true}
        maxWidth={columns.length > 3 ? 'xl' : 'sm'}
        open={open}
        onClose={handleClose}
        aria-labelledby='dialog-title'
        aria-describedby='dialog-description'
      >
        <DialogTitle id='dialog-title' data-testid={'sourceTableTitleId'}>
          {`${sourceName.toUpperCase()} - ${sourceVariant}`}
        </DialogTitle>
        <DialogContent>
          {isLoading ? (
            <Box sx={{ display: 'flex', justifyContent: 'center' }}>
              <CircularProgress />
            </Box>
          ) : error === '' ? (
            <SourceDialogTable api={api} columns={columns} rowList={rowList} />
          ) : (
            <div data-testid='errorMessageId'>{error}</div>
          )}
        </DialogContent>
        <DialogActions>
          <Button data-testid={'sourceTableCloseId'} onClick={handleClose}>
            Close
          </Button>
        </DialogActions>
      </Dialog>
    </div>
  );
}
