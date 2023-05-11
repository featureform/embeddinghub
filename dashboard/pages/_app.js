import Container from '@material-ui/core/Container';
import CssBaseline from '@material-ui/core/CssBaseline';
import { makeStyles, ThemeProvider } from '@material-ui/core/styles';
import React from 'react';
import ResourcesAPI from '../src/api/resources';
import BreadCrumbs from '../src/components/breadcrumbs/BreadCrumbs';
import ReduxStore from '../src/components/redux/store';
import ReduxWrapper from '../src/components/redux/wrapper';
import TopBar from '../src/components/topbar/TopBar';
import '../src/styles/base.css';
import theme from '../src/styles/theme';

const apiHandle = new ResourcesAPI();
const useStyles = makeStyles((theme) => ({
  pageContainer: {
    paddingLeft: theme.spacing(8),
    paddingRight: theme.spacing(8),
  },
}));

export const MyApp = ({ Component, pageProps }) => {
  const classes = useStyles();
  return (
    <React.StrictMode>
      <ReduxWrapper store={ReduxStore}>
        <ThemeWrapper>
          <TopBar className={classes.topbar} />
          <Container
            maxWidth='xl'
            className={classes.root}
            classes={{ maxWidthXl: classes.pageContainer }}
          >
            <BreadCrumbs />
            <Component {...pageProps} api={apiHandle} />
          </Container>
        </ThemeWrapper>
      </ReduxWrapper>
    </React.StrictMode>
  );
};

export const views = {
  RESOURCE_LIST: 'ResourceList',
  EMPTY: 'Empty',
};

export const ThemeWrapper = ({ children }) => (
  <ThemeProvider theme={theme}>
    <CssBaseline />
    {children}
  </ThemeProvider>
);

export default MyApp;

// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: https://bit.ly/CRA-PWA
// serviceWorker.unregister();
