import { Routes, Route } from "react-router-dom";
import { ErrorBoundary } from "./components/ErrorBoundary";
import Layout from "./components/Layout";
import Overview from "./pages/Overview";
import Tasks from "./pages/Tasks";
import Instances from "./pages/Instances";
import InstanceDetail from "./pages/InstanceDetail";
import Approvals from "./pages/Approvals";
import Sequences from "./pages/Sequences";
import SequenceDetail from "./pages/SequenceDetail";
import Cron from "./pages/Cron";
import Triggers from "./pages/Triggers";
import Operations from "./pages/Operations";
import Sessions from "./pages/Sessions";
import Plugins from "./pages/Plugins";
import Credentials from "./pages/Credentials";
import Pools from "./pages/Pools";
import Settings from "./pages/Settings";
import MobileSync from "./pages/MobileSync";
import Usage from "./pages/Usage";

export default function App() {
  return (
    <ErrorBoundary>
      <Routes>
        <Route element={<Layout />}>
          <Route index element={<Overview />} />
          <Route path="instances" element={<Instances />} />
          <Route path="instances/:id" element={<InstanceDetail />} />
          <Route path="approvals" element={<Approvals />} />
          <Route path="sequences" element={<Sequences />} />
          <Route path="sequences/:id" element={<SequenceDetail />} />
          <Route path="tasks" element={<Tasks />} />
          <Route path="cron" element={<Cron />} />
          <Route path="triggers" element={<Triggers />} />
          <Route path="operations" element={<Operations />} />
          <Route path="sessions" element={<Sessions />} />
          <Route path="usage" element={<Usage />} />
          <Route path="plugins" element={<Plugins />} />
          <Route path="credentials" element={<Credentials />} />
          <Route path="pools" element={<Pools />} />
          <Route path="mobile" element={<MobileSync />} />
          <Route path="settings" element={<Settings />} />
        </Route>
      </Routes>
    </ErrorBoundary>
  );
}
