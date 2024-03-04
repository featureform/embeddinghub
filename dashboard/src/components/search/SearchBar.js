import Container from '@mui/material/Container';
import Grid from '@mui/material/Grid';
import InputBase from '@mui/material/InputBase';
import { createStyles, makeStyles } from '@mui/styles';
import { useRouter } from 'next/router';
import React, { useEffect } from 'react';

const ENTER_KEY = 'Enter';

const useStyles = makeStyles((theme) =>
  createStyles({
    search: {
      display: 'flex',
      flexDirection: 'row',
      alignItems: 'center',
      padding: '0px',
      gap: '4px',
      position: 'absolute',
      width: '328px',
      height: '36px',
      left: `calc(50% - 328px/2)`,
      background: `#FFFFFF`,
      borderRadius: `28px`,
    },
    border: {
      border: `2px solid #D3D3D3`,
      borderRadius: 16,
      '&:hover': {
        border: `2px solid #D3D3D3`,
      },
      color: '#000000',
    },
    inputRoot: {
      borderRadius: 16,
      background: 'transparent',
      boxShadow: 'none',
      transition: theme.transitions.create('width'),
      width: '100%',
      display: 'flex',
      color: '#grey',
    },
    inputColor: {
      color: '#000000',
    },
    inputInputHome: {
      paddingLeft: theme.spacing(4),
      transition: theme.transitions.create('width'),
      background: 'transparent',
      boxShadow: 'none',
      padding: theme.spacing(1, 0.75, 0.4, 0),
      justifyContent: 'center',
      display: 'flex',
      alignSelf: 'flex-end',
      color: '#000000',
    },
    inputTopBar: {
      width: '100%',
      transition: theme.transitions.create('width'),
      background: 'transparent',
      boxShadow: 'none',
      alignSelf: 'center',
      color: '#000000',
    },
  })
);

const SearchBar = ({ homePage }) => {
  const classes = useStyles();
  const router = useRouter();
  const [searchText, setSearchText] = React.useState('');

  function handleSearch(event) {
    event.preventDefault();
    let uri = '/query?q=' + searchText?.trim();
    router.push(uri);
  }

  useEffect(() => {
    if (router.query) {
      if ('q' in router.query) {
        setSearchText(router.query.q);
      } else {
        setSearchText('');
      }
    }
  }, [router.query]);

  return (
    <div className={classes.search}>
      <Grid container item justifyContent='center' direction='row'>
        <Container className={classes.border}>
          <InputBase
            placeholder='Search'
            onChange={(event) => {
              const rawText = event.target.value;
              if (rawText === '') {
                // user is deleting the text field. allow this and clear out state
                setSearchText(rawText);
                return;
              }
              const searchText = event.target.value ?? '';
              if (searchText.trim()) {
                setSearchText(searchText);
              }
            }}
            value={searchText}
            onKeyDown={(event) => {
              if (event.key === ENTER_KEY && searchText) {
                handleSearch(event);
              }
            }}
            classes={{
              root: classes.inputRoot,
              input: homePage ? classes.inputInputHome : classes.inputTopBar,
            }}
            inputProps={{
              'aria-label': 'search',
              'data-testid': 'searchInputId',
            }}
          />
        </Container>
      </Grid>
    </div>
  );
};

export default SearchBar;
