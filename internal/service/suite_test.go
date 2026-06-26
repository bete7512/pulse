package service_test

import (
	"context"
	"testing"

	reposmocks "github.com/bete7512/pulse/internal/repos/mocks"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
)

// ServiceSuite is the shared harness for the service package's tests. SetupTest gives
// every test a fresh gomock controller and a fresh set of repo mocks, so cases never
// leak expectations into one another. A test builds the specific component it exercises
// (service.New / NewWatchdog / NewProjector) from these mocks.
type ServiceSuite struct {
	suite.Suite
	ctrl      *gomock.Controller
	events    *reposmocks.MockEventRepo
	jobs      *reposmocks.MockJobRepo
	live      *reposmocks.MockLivenessRepo
	proj      *reposmocks.MockProjectionRepo
	schedules *reposmocks.MockScheduleRepo
}

func TestServiceSuite(t *testing.T) { suite.Run(t, new(ServiceSuite)) }

func (s *ServiceSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T()) // auto-finishes via t.Cleanup (gomock v0.6)
	s.events = reposmocks.NewMockEventRepo(s.ctrl)
	s.jobs = reposmocks.NewMockJobRepo(s.ctrl)
	s.live = reposmocks.NewMockLivenessRepo(s.ctrl)
	s.proj = reposmocks.NewMockProjectionRepo(s.ctrl)
	s.schedules = reposmocks.NewMockScheduleRepo(s.ctrl)
}

func ctx() context.Context { return context.Background() }
