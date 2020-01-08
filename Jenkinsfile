kernelname = 'man_notebooker_integration_' + currentBuild.id

def installKernel = { medusaVersion ->
  def venvDir = getVenvDir(medusaVersion)
  pyInstall('ipykernel', venvDir)
  withActivate("python -m ipykernel install --name=${kernelname} --user",
               venvDir)
}

def unit = {
  pyTest 'tests/unit'
}

def integration = {
  withEnv(["NOTEBOOK_KERNEL_NAME=${kernelname}", "NOTEBOOKER_DISABLE_GIT='1'"]) {
    pyTest 'tests/integration', "-m 'not compress and not serial'"
  }
}

def sanity = {
  pyTest 'tests/sanity', "-m 'not compress and not serial'"
}

env.PYTHONPATH = '.:..'  // So that the src directory can be used as a dir


manPython('ts2-el7') {
    buildLabel='ts2-el7'
    labels='ts2-el7'
    buildPinnedEgg = true
    dockerPath = 'docker'
    medusaVersions = [
        build: ['36-1'],
        test: ['36-1'],
        docker: ['36-1']
    ]
    publicProject = true
    preBuildStages = [
        [name: 'Installing ipython kernel',
         body: installKernel
         ]
    ]
    testStages =  [
        [name: 'Sanity checks',
         body: sanity,
        ],
        [name: 'Unit',
         body: unit,
        ],
        [name: 'Integration',
         body: integration,
         workers: 1
        ],
    ]
}
