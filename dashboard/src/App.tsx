import { Routes, Route } from "react-router-dom";
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
import Settings from "./pages/Settings";

export default function App() {
  return (
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
        <Route path="settings" element={<Settings />} />
      </Route>
    </Routes>
  );
}
