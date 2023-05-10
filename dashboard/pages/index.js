import Head from 'next/head';
import HomePage from '../src/components/homepage/HomePage';

const IndexPage = () => {
  return (
    <div>
      <Head>
        <title>Featureform Dashboard</title>
      </Head>
      <HomePage />
    </div>
  );
};

export default IndexPage;
