import { useRouter } from 'next/router';
import ResourcesAPI from '../../../src/api/resources/Resources';
import EntityPage from '../../../src/components/entitypage/EntityPage';

const EntityPageRoute = () => {
  const router = useRouter();
  const { type, entity } = router.query;
  const apiHandle = new ResourcesAPI();

  return <EntityPage api={apiHandle} type={type} entity={entity} />;
};

export default EntityPageRoute;
